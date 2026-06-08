package middleware

import (
	"bytes"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"

	"github.com/gin-gonic/gin"
)

func newTestCaptureWriter(t *testing.T, limit int) (*captureWriter, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	cw := &captureWriter{
		ResponseWriter: c.Writer,
		buf:            &bytes.Buffer{},
		limit:          limit,
	}
	return cw, recorder
}

func TestSanitizeTextPlainAndEmpty(t *testing.T) {
	if got := sanitizeText(nil); got != "" {
		t.Fatalf("empty input: got %q", got)
	}
	if got := sanitizeText([]byte("hello 中文")); got != "hello 中文" {
		t.Fatalf("valid utf-8 should pass through, got %q", got)
	}
}

func TestSanitizeTextNulByteIsBinary(t *testing.T) {
	got := sanitizeText([]byte("abc\x00def"))
	if !strings.HasPrefix(got, "[binary data:") {
		t.Fatalf("NUL byte should yield placeholder, got %q", got)
	}
}

func TestSanitizeTextGarbageIsBinary(t *testing.T) {
	// 中段无效字节的真乱码(非截断产物)应给占位符
	got := sanitizeText([]byte{0xff, 0xfe, 'a', 0xff, 'b'})
	if !strings.HasPrefix(got, "[binary data:") {
		t.Fatalf("invalid utf-8 garbage should yield placeholder, got %q", got)
	}
}

// 截断切在多字节字符中间时,必须保留可读前缀而不是丢弃整个内容。
// "中" 为 3 字节;切 10 字节 = 3 个完整字符 + 1 个残字节,应得 "中文内"。
func TestSanitizeTextTruncatedCJKKeepsPrefix(t *testing.T) {
	full := []byte("中文内容截断测试")
	cut := full[:10]
	got := sanitizeText(cut)
	if got != "中文内" {
		t.Fatalf("truncated CJK should keep readable prefix %q, got %q", "中文内", got)
	}
}

// 4 字节 emoji 被截断时同样保留前缀("ab😀" 共 6 字节,切 5 字节丢残 emoji)。
func TestSanitizeTextTruncatedEmojiKeepsPrefix(t *testing.T) {
	got := sanitizeText([]byte("ab😀")[:5])
	if got != "ab" {
		t.Fatalf("truncated emoji should keep prefix %q, got %q", "ab", got)
	}
}

func TestClampColumn(t *testing.T) {
	if got := clampColumn("abc", 10); got != "abc" {
		t.Fatalf("short value should pass through, got %q", got)
	}
	if got := clampColumn(strings.Repeat("x", 300), 255); len(got) != 255 {
		t.Fatalf("over-length value should clamp to 255 bytes, got %d", len(got))
	}
	// NUL 注入(如 URL %00 解码进 Path)必须被消毒
	if got := clampColumn("ab\x00cd", 64); strings.ContainsRune(got, 0x00) {
		t.Fatalf("NUL must not survive clampColumn, got %q", got)
	}
	// 占位符比窄列还长时必须硬截,绝不能超出列宽
	if got := clampColumn("ab\x00cdefghijklmnopqr", 16); len(got) > 16 {
		t.Fatalf("clamped value exceeds column width: %d bytes %q", len(got), got)
	}
	// 多字节字符截断处修剪,不残留无效 UTF-8
	long := strings.Repeat("中", 100)
	got := clampColumn(long, 64)
	if len(got) > 64 || !strings.HasPrefix(long, got) {
		t.Fatalf("CJK clamp wrong: %d bytes %q", len(got), got)
	}
}

func TestRedactQuery(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		contains string
		absent   string
	}{
		{"empty", "", "", ""},
		{"gemini key", "key=AIzaSySECRET&alt=sse", "%5BREDACTED%5D", "AIzaSySECRET"},
		{"api_key", "api_key=sk-secret", "%5BREDACTED%5D", "sk-secret"},
		{"no sensitive", "alt=sse&foo=bar", "alt=sse", "%5BREDACTED%5D"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactQuery(tc.in)
			if tc.contains != "" && !strings.Contains(got, tc.contains) {
				t.Fatalf("redactQuery(%q) = %q, want contains %q", tc.in, got, tc.contains)
			}
			if tc.absent != "" && strings.Contains(got, tc.absent) {
				t.Fatalf("redactQuery(%q) = %q, must not contain %q", tc.in, got, tc.absent)
			}
		})
	}
}

func TestMarshalHeadersRedaction(t *testing.T) {
	headers := map[string][]string{
		"Authorization": {"Bearer sk-secret"},
		"X-Api-Key":     {"sk-another"},
		"Content-Type":  {"application/json"},
		"Accept":        {"a", "b"},
	}
	got := marshalHeaders(headers)
	if strings.Contains(got, "sk-secret") || strings.Contains(got, "sk-another") {
		t.Fatalf("sensitive header leaked: %s", got)
	}
	if !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("expected [REDACTED] marker: %s", got)
	}
	if !strings.Contains(got, "application/json") {
		t.Fatalf("normal header missing: %s", got)
	}
	if !strings.Contains(got, "a, b") {
		t.Fatalf("multi-value header should be joined: %s", got)
	}
}

func TestIsTextContentType(t *testing.T) {
	for ct, want := range map[string]bool{
		"application/json; charset=utf-8": true,
		"text/event-stream":               true,
		"text/plain":                      true,
		"application/xml":                 true,
		"audio/mpeg":                      false,
		"application/octet-stream":        false,
		"image/png":                       false,
	} {
		if got := isTextContentType(ct); got != want {
			t.Fatalf("isTextContentType(%q) = %v, want %v", ct, got, want)
		}
	}
}

func TestCaptureWriterBasicCapture(t *testing.T) {
	cw, recorder := newTestCaptureWriter(t, 1<<20)
	cw.Header().Set("Content-Type", "application/json")
	cw.WriteHeader(201)
	if _, err := cw.Write([]byte(`{"a":`)); err != nil {
		t.Fatal(err)
	}
	if _, err := cw.WriteString(`1}`); err != nil {
		t.Fatal(err)
	}
	body, truncated, status, headers := cw.snapshot()
	if body != `{"a":1}` {
		t.Fatalf("captured %q", body)
	}
	if truncated {
		t.Fatal("should not be truncated")
	}
	// 状态码与 header 必须来自锁内快照,且与实际发送一致
	if status != 201 {
		t.Fatalf("snapshot status = %d, want 201", status)
	}
	if ct := headers["Content-Type"]; len(ct) == 0 || ct[0] != "application/json" {
		t.Fatalf("snapshot headers missing content-type: %v", headers)
	}
	// 真实 writer 收到完整数据,状态透传
	if recorder.Body.String() != `{"a":1}` {
		t.Fatalf("real writer got %q", recorder.Body.String())
	}
	if cw.Status() != 201 {
		t.Fatalf("status passthrough = %d", cw.Status())
	}
}

func TestCaptureWriterTruncation(t *testing.T) {
	cw, recorder := newTestCaptureWriter(t, 5)
	cw.Header().Set("Content-Type", "text/plain")
	_, _ = cw.Write([]byte("abc"))
	_, _ = cw.Write([]byte("defgh")) // 超限,只再捕 2 字节
	_, _ = cw.Write([]byte("ijk"))  // 已截断,忽略
	body, truncated, _, _ := cw.snapshot()
	if body != "abcde" {
		t.Fatalf("captured %q, want abcde", body)
	}
	if !truncated {
		t.Fatal("expect truncated")
	}
	// 透传不受截断影响
	if recorder.Body.String() != "abcdefghijk" {
		t.Fatalf("real writer got %q", recorder.Body.String())
	}
}

func TestCaptureWriterSkipsBinaryContentType(t *testing.T) {
	cw, recorder := newTestCaptureWriter(t, 1<<20)
	cw.Header().Set("Content-Type", "audio/mpeg")
	_, _ = cw.Write([]byte{0x01, 0x02, 0x03})
	body, _, _, headers := cw.snapshot()
	if body != "" {
		t.Fatalf("binary content-type should not be captured, got %q", body)
	}
	// 即使跳过 body 捕获,header 快照仍应存在
	if ct := headers["Content-Type"]; len(ct) == 0 || ct[0] != "audio/mpeg" {
		t.Fatalf("header snapshot missing for skipped body: %v", headers)
	}
	if recorder.Body.Len() != 3 {
		t.Fatalf("real writer should still receive bytes, got %d", recorder.Body.Len())
	}
}

// snapshot 之后迟到的流式写入(5s 超时逃逸场景)必须安全且不影响已取快照。
func TestCaptureWriterLateWriteAfterSnapshot(t *testing.T) {
	cw, recorder := newTestCaptureWriter(t, 1<<20)
	cw.Header().Set("Content-Type", "text/event-stream")
	_, _ = cw.Write([]byte("data: early\n\n"))
	body, _, _, _ := cw.snapshot()
	if body != "data: early\n\n" {
		t.Fatalf("captured %q", body)
	}
	// 迟到写入:不 panic、透传仍工作、快照不变
	if _, err := cw.Write([]byte("data: late\n\n")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(recorder.Body.String(), "late") {
		t.Fatal("late write must still reach the real writer")
	}
	bodyAgain, _, _, _ := cw.snapshot()
	if bodyAgain != "" {
		t.Fatalf("second snapshot should be empty, got %q", bodyAgain)
	}
}

// 无任何写出的响应(abort 无 body 等):snapshot 不得为取 header 而
// range 活 map(与迟到首写竞争是 runtime.throw),headers 应为 nil。
func TestCaptureWriterSnapshotWithoutWrite(t *testing.T) {
	cw, _ := newTestCaptureWriter(t, 1<<20)
	cw.Header().Set("X-Test", "v")
	body, truncated, status, headers := cw.snapshot()
	if body != "" || truncated {
		t.Fatalf("no-write snapshot: body=%q truncated=%v", body, truncated)
	}
	if status == 0 {
		t.Fatal("status should fall back to underlying writer default")
	}
	if headers != nil {
		t.Fatalf("zero-write snapshot must not touch the live header map, got %v", headers)
	}
}

// 零写出 + 并发首写:snapshot 的 headerSnap==nil 分支绝不能 range
// 活 header map —— 迟到的 c.Render 直接写 map 不经过任何锁,
// concurrent map range+write 是 runtime.throw。本测试无 warmup 写,
// 专门逼出该分支(TTFT > 5s 的流式请求正是这个形态)。
func TestCaptureWriterSnapshotRacesFirstWrite(t *testing.T) {
	for i := 0; i < 50; i++ {
		cw, _ := newTestCaptureWriter(t, 1<<20)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 模拟迟到的 c.Render 首写:先写 header map,再写 body
			cw.Header().Set("Content-Type", "text/event-stream")
			_, _ = cw.Write([]byte("data: first\n\n"))
		}()
		_, _, _, _ = cw.snapshot() // 与首写并发
		wg.Wait()
	}
}

// go test -race 下并发 Write/Render(写 header map)与 snapshot:
// 证明锁与 header 快照确实关闭了 stream_scanner 5 秒超时逃逸导致的
// 写/读竞争(包括 concurrent map read+write)。
func TestCaptureWriterConcurrentWriteAndSnapshot(t *testing.T) {
	cw, _ := newTestCaptureWriter(t, 1<<20)
	cw.Header().Set("Content-Type", "text/event-stream")
	_, _ = cw.Write([]byte("data: warmup\n\n")) // 触发 header 快照(首次写)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				// 模拟迟到的 c.Render: 既写 header map 又写 body
				cw.Header().Set("Content-Type", "text/event-stream")
				_, _ = cw.Write([]byte("data: chunk\n\n"))
			}
		}
	}()

	for i := 0; i < 100; i++ {
		_, _, _, headers := cw.snapshot()
		for range headers { // 消费快照(等价 marshalHeaders 的遍历)
		}
	}
	close(stop)
	wg.Wait()
}

// ---- fake BodyStorage ----

type fakeBodyStorage struct {
	reader  *bytes.Reader
	data    []byte
	seekErr error
	isDisk  bool
}

func newFakeBodyStorage(data []byte) *fakeBodyStorage {
	return &fakeBodyStorage{reader: bytes.NewReader(data), data: data}
}

func (f *fakeBodyStorage) Read(p []byte) (int, error) { return f.reader.Read(p) }
func (f *fakeBodyStorage) Seek(offset int64, whence int) (int64, error) {
	if f.seekErr != nil {
		return 0, f.seekErr
	}
	return f.reader.Seek(offset, whence)
}
func (f *fakeBodyStorage) Close() error           { return nil }
func (f *fakeBodyStorage) Bytes() ([]byte, error) { return f.data, nil }
func (f *fakeBodyStorage) Size() int64            { return int64(len(f.data)) }
func (f *fakeBodyStorage) IsDisk() bool           { return f.isDisk }

var _ common.BodyStorage = (*fakeBodyStorage)(nil)

func TestReadCachedRequestBodyBranches(t *testing.T) {
	gin.SetMode(gin.TestMode)
	newCtx := func() *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		return c
	}

	t.Run("no storage", func(t *testing.T) {
		c := newCtx()
		body, truncated := readCachedRequestBody(c, 100)
		if body != "" || truncated {
			t.Fatalf("got %q %v", body, truncated)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		c := newCtx()
		c.Set(common.KeyBodyStorage, "not a storage")
		body, truncated := readCachedRequestBody(c, 100)
		if body != "" || truncated {
			t.Fatalf("got %q %v", body, truncated)
		}
	})

	t.Run("empty storage", func(t *testing.T) {
		c := newCtx()
		c.Set(common.KeyBodyStorage, newFakeBodyStorage(nil))
		body, truncated := readCachedRequestBody(c, 100)
		if body != "" || truncated {
			t.Fatalf("got %q %v", body, truncated)
		}
	})

	t.Run("seek error", func(t *testing.T) {
		c := newCtx()
		fs := newFakeBodyStorage([]byte("data"))
		fs.seekErr = errors.New("io broken")
		c.Set(common.KeyBodyStorage, fs)
		body, truncated := readCachedRequestBody(c, 100)
		if body != "" || truncated {
			t.Fatalf("got %q %v", body, truncated)
		}
	})

	t.Run("happy path", func(t *testing.T) {
		c := newCtx()
		fs := newFakeBodyStorage([]byte(`{"model":"x"}`))
		c.Set(common.KeyBodyStorage, fs)
		body, truncated := readCachedRequestBody(c, 100)
		if body != `{"model":"x"}` || truncated {
			t.Fatalf("got %q %v", body, truncated)
		}
		// 读取后必须复位偏移(防御后续读取)
		if pos, _ := fs.reader.Seek(0, io.SeekCurrent); pos != 0 {
			t.Fatalf("offset not reset, at %d", pos)
		}
	})

	t.Run("truncated over limit", func(t *testing.T) {
		c := newCtx()
		c.Set(common.KeyBodyStorage, newFakeBodyStorage([]byte("0123456789")))
		body, truncated := readCachedRequestBody(c, 4)
		if body != "0123" || !truncated {
			t.Fatalf("got %q %v", body, truncated)
		}
	})
}

// buildRelayRecord 是凭证防泄漏的最后一道关口:字段映射、脱敏、消毒
// 必须在装配层整体生效,而不仅是叶子函数各自正确。
func TestBuildRelayRecordMapsFieldsAndRedacts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions?key=QUERYSECRET&alt=sse", nil)
	c.Request.Header.Set("Authorization", "Bearer sk-headersecret")
	c.Request.Header.Set("Content-Type", "application/json")

	common.SetContextKey(c, constant.ContextKeyUserId, 42)
	common.SetContextKey(c, constant.ContextKeyUserName, "alice")
	common.SetContextKey(c, constant.ContextKeyTokenId, 7)
	common.SetContextKey(c, constant.ContextKeyChannelId, 3)
	common.SetContextKey(c, constant.ContextKeyOriginalModel, "claude-3-opus")
	common.SetContextKey(c, constant.ContextKeyUsingGroup, "default")
	common.SetContextKey(c, constant.ContextKeyIsStream, true)
	c.Set(common.KeyBodyStorage, newFakeBodyStorage([]byte(`{"model":"claude-3-opus"}`)))

	cw := &captureWriter{ResponseWriter: c.Writer, buf: &bytes.Buffer{}, limit: 1 << 20}
	cw.Header().Set("Content-Type", "application/json")
	cw.WriteHeader(200)
	_, _ = cw.Write([]byte(`{"ok":true}`))

	rec := buildRelayRecord(c, cw, time.Now(), 1<<20)

	if strings.Contains(rec.RequestHeaders, "sk-headersecret") {
		t.Fatalf("auth header leaked into record: %s", rec.RequestHeaders)
	}
	if !strings.Contains(rec.RequestHeaders, "[REDACTED]") {
		t.Fatalf("request headers not redacted: %s", rec.RequestHeaders)
	}
	if strings.Contains(rec.Query, "QUERYSECRET") {
		t.Fatalf("query key leaked into record: %s", rec.Query)
	}
	if rec.RequestBody != `{"model":"claude-3-opus"}` {
		t.Fatalf("request body = %q", rec.RequestBody)
	}
	if rec.ResponseBody != `{"ok":true}` {
		t.Fatalf("response body = %q", rec.ResponseBody)
	}
	if rec.StatusCode != 200 {
		t.Fatalf("status = %d", rec.StatusCode)
	}
	if !strings.Contains(rec.ResponseHeaders, "application/json") {
		t.Fatalf("response headers = %q", rec.ResponseHeaders)
	}
	if rec.UserId != 42 || rec.Username != "alice" || rec.TokenId != 7 || rec.ChannelId != 3 {
		t.Fatalf("identity fields wrong: %+v", rec)
	}
	if rec.ModelName != "claude-3-opus" || rec.Group != "default" || !rec.IsStream {
		t.Fatalf("relay fields wrong: %+v", rec)
	}
	if rec.Method != "POST" || rec.Path != "/v1/chat/completions" {
		t.Fatalf("request line wrong: %+v", rec)
	}
	if rec.DurationMs < 0 {
		t.Fatalf("duration = %d", rec.DurationMs)
	}
}

// 环境变量门控:未启用时绝不包装 writer(零开销路径),启用时包装。
func TestRelayRecordEnvGate(t *testing.T) {
	gin.SetMode(gin.TestMode)

	runProbe := func(t *testing.T) bool {
		t.Helper()
		engine := gin.New()
		engine.Use(RelayRecord())
		var wrapped bool
		engine.GET("/probe", func(c *gin.Context) {
			_, wrapped = c.Writer.(*captureWriter)
			c.String(200, "ok")
		})
		recorder := httptest.NewRecorder()
		engine.ServeHTTP(recorder, httptest.NewRequest("GET", "/probe", nil))
		if recorder.Code != 200 || recorder.Body.String() != "ok" {
			t.Fatalf("probe response broken: %d %q", recorder.Code, recorder.Body.String())
		}
		return wrapped
	}

	t.Run("disabled by default", func(t *testing.T) {
		t.Setenv("RELAY_RECORD_ENABLED", "")
		if runProbe(t) {
			t.Fatal("writer must NOT be wrapped when disabled")
		}
	})

	t.Run("enabled", func(t *testing.T) {
		t.Setenv("RELAY_RECORD_ENABLED", "true")
		if !runProbe(t) {
			t.Fatal("writer must be wrapped when enabled")
		}
	})
}
