package auth

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 一个坏文件不能让整个服务起不来：此前 LoadDir 任一文件解析失败都返回错误，
// 启动时直接退出，等于全站不可用。
func TestLoadDirSkipsCorruptFile(t *testing.T) {
	dir := t.TempDir()
	good := New("u1", "n1", "d1", "tok", "ak", "sk", time.Now().Add(time.Hour).Format(time.RFC3339), "rt", "v")
	if err := SaveNew(dir, good); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "codearts-broken.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	// 非凭证文件不应参与加载。
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("坏文件应被跳过而不是整体失败: %v", err)
	}
	if len(loaded) != 1 || loaded[0].UserID != "u1" {
		t.Fatalf("loaded=%d", len(loaded))
	}

	// 全部文件都坏时才报错（否则调用方会以为「没有账号」而静默跑空池）。
	allBroken := t.TempDir()
	if err := os.WriteFile(filepath.Join(allBroken, "codearts-x.json"), []byte("{oops"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDir(allBroken); err == nil {
		t.Fatal("全部凭证都损坏时应返回错误")
	}
}

// 过期时间缺失/无法解析时，剩余有效期必须报「未知」（0），不能返回 24h：
// 那会把面板和刷新判断建立在假数据上。
func TestRemainingUnknownExpirationReturnsZero(t *testing.T) {
	for _, expiration := range []string{"", "not-a-time"} {
		a := New("u", "n", "d", "tok", "ak", "sk", expiration, "", "")
		if got := a.Remaining(); got != 0 {
			t.Errorf("expiration=%q Remaining=%s，应为 0", expiration, got)
		}
		if a.ExpirationKnown() {
			t.Errorf("expiration=%q 应报告未知", expiration)
		}
		if a.Expired() {
			t.Errorf("expiration=%q 未知不等于过期", expiration)
		}
	}
	a := New("u", "n", "d", "tok", "ak", "sk", time.Now().Add(time.Hour).Format(time.RFC3339), "", "")
	if !a.ExpirationKnown() || a.Remaining() <= 0 || a.Expired() {
		t.Errorf("已知过期时间应正常计算: known=%v remaining=%s", a.ExpirationKnown(), a.Remaining())
	}
}
