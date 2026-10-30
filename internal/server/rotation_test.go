package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"codearts2api/internal/auth"
	"codearts2api/internal/pool"
	"codearts2api/internal/upstream"
)

// 多账号池里模型注册/套餐并不一致：第一个账号说「没这个模型」时必须换号重试，
// 而不是直接给客户端 404。删除目录选择器后，轮转本身就是能力探测。
func TestModelUnavailableRotatesToAccountThatHasIt(t *testing.T) {
	const notRegistered = "data: {\"text\":\"[DONE]\",\"error_code\":\"InferHub.002002009.404\",\"error_msg\":\"The model is not registered, please request other model\"}\n\n"
	const okStream = "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"

	var hits []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("x-auth-token")
		hits = append(hits, token)
		w.Header().Set("Content-Type", "text/event-stream")
		if token == "tok-a" {
			_, _ = io.WriteString(w, notRegistered)
			return
		}
		_, _ = io.WriteString(w, okStream)
	}))
	defer backend.Close()

	p := rotationPool(t, backend.URL)
	h := NewHandler(Config{Pool: p, DefaultModel: "glm-5.2", MaxRotate: 2, ErrThreshold: 1, ErrCooldown: time.Minute})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"GLM-5.2-ArkTS-SPARK","messages":[{"role":"user","content":"hi"}]}`)))

	if rec.Code != http.StatusOK {
		t.Fatalf("应换到有该模型的账号并成功，status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"content":"OK"`) {
		t.Fatalf("body=%s", rec.Body.String())
	}
	if len(hits) != 2 || hits[0] != "tok-a" || hits[1] != "tok-b" {
		t.Errorf("应先试 a 再换 b，got=%v", hits)
	}
	// 能力问题不是账号故障：a 必须仍然健康，只记「这个账号没有该模型」。
	if !p.Healthy("acct-a") {
		t.Error("模型未注册不得冷却账号")
	}
	if st, _ := modelAvailability("acct-a", "GLM-5.2-ArkTS-SPARK"); st != availUnusable {
		t.Errorf("a 应记为模型不可用，got=%v", st)
	}
}

// 所有账号都没有该模型：轮转跑完后回 404 model_not_available（而不是 503），
// 且不得冷却任何一个健康账号。
func TestModelUnavailableOnAllAccountsReturns404(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"text\":\"[DONE]\",\"error_code\":\"InferHub.002002009.404\",\"error_msg\":\"The model is not registered\"}\n\n")
	}))
	defer backend.Close()

	p := rotationPool(t, backend.URL)
	h := NewHandler(Config{Pool: p, DefaultModel: "glm-5.2", MaxRotate: 2, ErrThreshold: 1, ErrCooldown: time.Minute})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"GLM-5.2-ArkTS-SPARK","messages":[{"role":"user","content":"hi"}]}`)))

	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "model_not_available") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if total, healthy, disabled, cooling := p.Stats(); total != 2 || healthy != 2 || disabled != 0 || cooling != 0 {
		t.Fatalf("模型级错误改变了账号状态: total=%d healthy=%d disabled=%d cooling=%d", total, healthy, disabled, cooling)
	}
}

func rotationPool(t *testing.T, endpoint string) *pool.Pool {
	t.Helper()
	auths := []*auth.Auth{
		auth.New("acct-a", "a", "domain", "tok-a", "ak", "sk", time.Now().Add(time.Hour).Format(time.RFC3339), "", ""),
		auth.New("acct-b", "b", "domain", "tok-b", "ak", "sk", time.Now().Add(time.Hour).Format(time.RFC3339), "", ""),
	}
	p, err := pool.New(auths, pool.Config{MaxConcurrent: 1, ErrThreshold: 1}, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, acct := range p.Accounts() {
		acct.Client = upstream.NewWithChatEndpoint(2*time.Second, endpoint)
	}
	return p
}

// 若账号携带了福利标记导致上游返回 4004.200 benefit not found，
// 应自动摘除福利头重试并成功。
func TestChatCompletionsRecoversFromBenefitNotFound(t *testing.T) {
	var attempts int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.Header().Set("Content-Type", "text/event-stream")
		if r.Header.Get("maas_type") == "benefit" {
			_, _ = io.WriteString(w, "data: {\"text\":\"[DONE]\",\"error_code\":\"InferHub.4004.200\",\"error_msg\":\"benefit not found\"}\n\n")
			return
		}
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"OK\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer backend.Close()

	a := auth.New("acct-benefit", "a", "domain", "tok-a", "ak", "sk", time.Now().Add(time.Hour).Format(time.RFC3339), "", "")
	p, err := pool.New([]*auth.Auth{a}, pool.Config{MaxConcurrent: 1}, "")
	if err != nil {
		t.Fatal(err)
	}
	p.Accounts()[0].Client = upstream.NewWithChatEndpoint(5*time.Second, backend.URL)
	// 在该账号目录中故意标记为福利模型
	upstream.SetAccountModels("acct-benefit", []upstream.ModelInfo{{ID: "glm-5.3", Benefit: true}})

	h := NewHandler(Config{Pool: p, DefaultModel: "glm-5.3", MaxRotate: 1})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.3","messages":[{"role":"user","content":"hi"}]}`)))

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if attempts != 2 {
		t.Fatalf("应在同账号上自动摘除福利头重试，attempts=%d, want 2", attempts)
	}
}
