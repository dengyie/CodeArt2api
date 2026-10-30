package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"codearts2api/internal/auth"
	"codearts2api/internal/pool"
	"codearts2api/internal/upstream"
)

// usage 只透传上游真实给出的值：上游没给就不出现 usage 字段。
// 早期实现会在缺失时用估算值（甚至伪造 usage:null）填充，客户端据此计费
// 会拿到与上游无关的数字，因此这里逐条钉死「给了什么就回什么」。
func TestChatCompletionsUsage(t *testing.T) {
	const answer = "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\",\"content\":\"OK\"},\"finish_reason\":\"stop\"}]}\n\n"
	const nativeUsage = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":20,\"total_tokens\":30,\"completion_tokens_details\":{\"reasoning_tokens\":8}}}\n\n"
	const done = "data: [DONE]\n\n"
	exact := `{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30,"completion_tokens_details":{"reasoning_tokens":8}}`
	includeUsage := `,"stream_options":{"include_usage":true}`
	for _, tc := range []struct {
		name, options, wire, want string
		stream                    bool
		truncated                 bool
	}{
		// 上游没有 usage：即使客户端要 include_usage，也不凭空造一个。
		{"no_upstream_usage", includeUsage, answer + done, "", true, false},
		{"native", includeUsage, answer + nativeUsage + done, exact, true, false},
		// 客户端没要 usage（缺省/false/null）：有也不发 usage chunk。
		{"omitted", "", answer + nativeUsage + done, "", true, false},
		{"disabled", `,"stream_options":{"include_usage":false}`, answer + nativeUsage + done, "", true, false},
		{"null", `,"stream_options":null`, answer + done, "", true, false},
		// 非流式：有就带，没有就省略。
		{"nonstream_no_usage", "", answer + done, "", false, false},
		{"nonstream_native", "", answer + nativeUsage + done, exact, false, false},
		// 老版 CodeArts 事件（event: done，无 usage 字段）。
		{"legacy_done", includeUsage, "event: onAnswer\ndata: {\"text\":\"OK\"}\n\nevent: done\ndata: {\"error_code\":\"0\"}\n\n", "", true, false},
		// 上游中途断流：判定为截断，不补 [DONE]。
		{"eof", includeUsage, answer, "", true, true},
		{"error", includeUsage, answer + "event: done\ndata: {\"error_code\":\"failed\",\"error_msg\":\"failed\"}\n\n", "", true, false},
		{"tools", includeUsage, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call1\",\"function\":{\"name\":\"test\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n" + nativeUsage + done, exact, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, tc.wire)
			}))
			defer up.Close()
			a := auth.New("usage-account", "tester", "domain", "token", "ak", "sk", time.Now().Add(time.Hour).Format(time.RFC3339), "", "")
			p, err := pool.New([]*auth.Auth{a}, pool.Config{MaxConcurrent: 1}, "")
			if err != nil {
				t.Fatal(err)
			}
			p.Accounts()[0].Client = upstream.NewWithChatEndpoint(5*time.Second, up.URL)
			h := NewHandler(Config{Pool: p, DefaultModel: "glm-5.2", MaxRotate: 1})
			stream := "false"
			if tc.stream {
				stream = "true"
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","stream":`+stream+`,"messages":[{"role":"user","content":"hi"}]`+tc.options+`}`))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			var got map[string]any
			if !tc.stream {
				var body map[string]any
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				got, _ = body["usage"].(map[string]any)
			} else {
				data := strings.Split(strings.TrimSpace(rec.Body.String()), "\n\n")
				hasDone := data[len(data)-1] == "data: [DONE]"
				if tc.truncated {
					if hasDone {
						t.Fatalf("截断的流不得补 [DONE]: %s", rec.Body.String())
					}
					if !strings.Contains(rec.Body.String(), `"content":"OK"`) {
						t.Fatalf("截断前已产出的内容应当已经下发: %s", rec.Body.String())
					}
					return
				}
				if !hasDone {
					t.Fatalf("missing DONE: %s", rec.Body.String())
				}
				usageCount := 0
				var id any
				for i, event := range data[:len(data)-1] {
					if strings.HasPrefix(event, "event: error") {
						continue
					}
					var chunk map[string]any
					if err := json.Unmarshal([]byte(strings.TrimPrefix(event, "data: ")), &chunk); err != nil {
						t.Fatal(err)
					}
					if id == nil {
						id = chunk["id"]
					}
					if chunk["id"] != id || chunk["model"] != "glm-5.2" || chunk["object"] != "chat.completion.chunk" {
						t.Fatalf("bad metadata: %#v", chunk)
					}
					// usage 只能在最后一个 chunk 出现，且该 chunk 的 choices 必须为空。
					if u, ok := chunk["usage"].(map[string]any); ok {
						usageCount++
						got = u
						choices, ok := chunk["choices"].([]any)
						if !ok || len(choices) != 0 || i != len(data)-2 {
							t.Fatalf("usage must be empty-choices chunk before DONE: %#v", chunk)
						}
					}
				}
				if usageCount > 1 {
					t.Fatalf("duplicate usage chunks: %d", usageCount)
				}
				if tc.name == "error" && !strings.Contains(rec.Body.String(), "event: error") {
					t.Fatal("missing error event")
				}
				if tc.name == "tools" && !strings.Contains(rec.Body.String(), `"finish_reason":"tool_calls"`) {
					t.Fatal("missing tool finish")
				}
			}
			var want map[string]any
			if tc.want != "" {
				if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("usage=%#v want=%#v; body=%s", got, want, rec.Body.String())
			}
		})
	}
}
