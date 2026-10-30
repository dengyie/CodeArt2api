package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"codearts2api/internal/auth"
	"codearts2api/internal/pool"
	"codearts2api/internal/upstream"
)

// 并发请求会同时调 saveChats：既不能在锁内 Marshal（并发 map 读写直接 fatal），
// 也不能让两个 goroutine 抢写同一个 .tmp 而写出半个 JSON。
func TestSaveChatsConcurrentRequests(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "chats.json")
	h := NewHandler(Config{ConvStateFile: statePath})

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				key := string(rune('a'+i%26)) + "-" + string(rune('a'+j%26))
				h.convMu.Lock()
				h.chats[key] = "chat-" + key
				h.rememberConvLocked("conv-"+key, key)
				h.convMu.Unlock()
				h.saveChats()
			}
		}(i)
	}
	wg.Wait()

	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("并发写盘产生了损坏的 JSON: %v\n%s", err, raw)
	}
	if len(got) == 0 {
		t.Fatal("状态文件为空")
	}
	if _, err := os.Stat(statePath + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("临时文件未清理: %v", err)
	}
}

// 缺 DPoP 私钥的登录结果不能落盘：refresh_token 与签发时的公钥绑定，
// 这种凭证 2 小时后永远刷不动，只会变成一个「看起来健康」的死账号。
func TestSaveLoginResultRejectsRefreshTokenWithoutDPoP(t *testing.T) {
	dir := t.TempDir()
	p, err := pool.New(nil, pool.Config{}, "")
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(Config{AuthDir: dir, Pool: p})
	tok := &upstream.TokenResponse{
		UserID:       "u-dpop",
		UserName:     "tester",
		DomainID:     "d",
		RefreshToken: "rt",
		Credentials: upstream.Credentials{
			AccessKeyID: "ak", SecretAccessKey: "sk", SecurityToken: "st",
			Expiration: time.Now().Add(2 * time.Hour).Format(time.RFC3339),
		},
	}
	if err := h.saveLoginResult(tok, "verifier", nil); err == nil {
		t.Fatal("缺 DPoP 私钥时应拒绝保存")
	}
	if loaded, err := auth.LoadDir(dir); err != nil || len(loaded) != 0 {
		t.Fatalf("拒绝保存时不得落盘: loaded=%d err=%v", len(loaded), err)
	}

	// 带上私钥则正常落盘。
	key, err := upstream.NewDPoPPrivateJWK()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.saveLoginResult(tok, "verifier", key); err != nil {
		t.Fatalf("带 DPoP 私钥应保存成功: %v", err)
	}
	loaded, err := auth.LoadDir(dir)
	if err != nil || len(loaded) != 1 || loaded[0].DPoPPrivateJWK()["d"] == "" {
		t.Fatalf("落盘结果应包含 DPoP 私钥: loaded=%d err=%v", len(loaded), err)
	}
}

// 客户端断开（取消请求）不是账号故障：既不能冷却账号，也不能把错误计数
// 推到阈值把健康账号踢下线。
func TestClientCancelDoesNotPenalizeAccount(t *testing.T) {
	for _, stream := range []bool{false, true} {
		name := "nonstream"
		if stream {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
			}))
			defer backend.Close()

			p := rotationPool(t, backend.URL)
			h := NewHandler(Config{Pool: p, DefaultModel: "glm-5.2", MaxRotate: 1, ErrThreshold: 1, ErrCooldown: time.Minute})

			ctx, cancel := context.WithCancel(context.Background())
			cancel() // 模拟用户点「停止」：请求还没发出去就已经取消
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
				strings.NewReader(`{"model":"glm-5.2","stream":`+boolLit(stream)+`,"messages":[{"role":"user","content":"hi"}]}`)).WithContext(ctx)
			h.ServeHTTP(httptest.NewRecorder(), req)

			if total, healthy, disabled, cooling := p.Stats(); total != 2 || healthy != 2 || disabled != 0 || cooling != 0 {
				t.Fatalf("客户端取消不得处罚账号: total=%d healthy=%d disabled=%d cooling=%d", total, healthy, disabled, cooling)
			}
		})
	}
}

func boolLit(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
