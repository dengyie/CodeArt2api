package server

import (
	"testing"

	"codearts2api/internal/upstream"
)

func TestModelEntriesAliasesStaticAndBenefit(t *testing.T) {
	entries := modelEntries([]upstream.ModelInfo{
		{ID: "GLM-5.2", ContextWindow: 202752, MaxTokens: 8192},
		{ID: "deepseek-v4-pro-0813", ContextWindow: 1048576, Benefit: true},
	})
	byID := map[string]map[string]any{}
	for _, e := range entries {
		id, _ := e["id"].(string)
		if byID[id] != nil {
			t.Fatalf("/v1/models 出现重复 ID: %s", id)
		}
		byID[id] = e
	}

	// 精确 ID 与小写别名都要在（老客户端习惯用小写）。
	for _, want := range []string{"GLM-5.2", "glm-5.2", "deepseek-v4-pro-0813"} {
		if byID[want] == nil {
			t.Errorf("缺少模型条目 %q，got=%v", want, keysOf(byID))
		}
	}
	if byID["GLM-5.2"]["context_length"] != int64(202752) {
		t.Errorf("context_length 应取自上游目录，got=%v", byID["GLM-5.2"])
	}
	if byID["GLM-5.2"]["max_output_tokens"] != int64(8192) {
		t.Errorf("max_output_tokens 应取自上游目录，got=%v", byID["GLM-5.2"])
	}
	if byID["deepseek-v4-pro-0813"]["benefit"] != true {
		t.Error("福利模型应在 /v1/models 标记 benefit")
	}

	// 动态目录缺失的模型由静态表补位（精确 ID + 小写别名），福利标记跟随静态表。
	for _, want := range []string{"Qwen3-VL-235B", "qwen3-vl-235b", "glm-5.3-flash", "deepseek-v4-flash-0731"} {
		if byID[want] == nil {
			t.Errorf("静态兜底缺少 %q", want)
		}
	}
	if byID["qwen3-vl-235b"]["benefit"] != nil {
		t.Error("内置模型不应标记 benefit")
	}
	if byID["glm-5.3-flash"]["benefit"] != true {
		t.Error("静态兜底里的福利模型应保留 benefit 标记")
	}

	// 动态目录整体失败（无账号可用）时仍返回完整静态表 + 别名。
	fallback := modelEntries(nil)
	if len(fallback) < len(staticModels) {
		t.Fatalf("静态兜底条目过少: %d", len(fallback))
	}
	seen := map[string]bool{}
	for _, e := range fallback {
		id, _ := e["id"].(string)
		if seen[id] {
			t.Fatalf("静态兜底出现重复 ID: %s", id)
		}
		seen[id] = true
	}
	if !seen["GLM-5.2"] || !seen["glm-5.2"] {
		t.Errorf("静态兜底也要给大写 ID 补小写别名，got=%v", keysOf(seen))
	}
}

func keysOf[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
