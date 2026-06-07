package middleware

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/model"

	"github.com/gin-gonic/gin"
)

var relayRecordDropCount int64

// 请求体/响应体各自的捕获上限。
// 长上下文轨迹(多轮 agent 会话)的请求体携带完整历史,100K-200K token
// 上下文约 0.5-1.2MB JSON,含截图(base64)可达数 MB —— 这些恰恰是
// 最有价值的记录,上限过低会正好截断它们(truncated 记录下游只能废弃)。
// 16MB 覆盖满上下文 + 多张截图;内存安全由 model 侧的队列字节预算兜底。
const relayRecordMaxBodyBytes = 16 << 20

// captureWriter 包装 gin.ResponseWriter,在透传写入的同时把响应体
// 复制进内存 buffer(有上限)。嵌入接口使 Status()/Size()/Written()/
// Flush()/Hijack()/CloseNotify()/Pusher() 等全部自动透传到真实 writer,
// 外层 gin logger / StatsMiddleware 在 unwind 后读取状态不受影响。
//
// mu 的存在是因为流式响应的写 goroutine 在超时场景下可能于 handler
// 返回后仍调用 Write/Render(relay/helper/stream_scanner.go 的 wg.Wait
// 有 5 秒超时逃逸)。c.Render 每次都会写 header map(custom-event.go
// 的 WriteContentType),所以请求结束后绝不能再读活的 Header()/Status()
// —— concurrent map read+write 是 runtime.throw,recover 兜不住。
// 因此 status/header 都在锁内自行维护快照:WriteHeader 记录状态码,
// 首次写入时深拷贝 header(此刻 header 已随首字节提交给客户端,即
// 实际发送的版本);snapshot 只返回这些快照,不触碰底层 writer。
type captureWriter struct {
	gin.ResponseWriter
	mu         sync.Mutex
	buf        *bytes.Buffer
	limit      int
	captured   int
	truncated  bool
	skip       bool // 响应 Content-Type 非文本类时跳过捕获
	checked    bool
	statusCode int                 // 锁内维护的状态码快照
	headerSnap map[string][]string // 首次写入时的 header 深拷贝
}

// copyHeaderLocked 深拷贝底层 writer 的 header(须在 mu 持有时调用)。
func (w *captureWriter) copyHeaderLocked() {
	src := w.ResponseWriter.Header()
	dst := make(map[string][]string, len(src))
	for key, values := range src {
		copied := make([]string, len(values))
		copy(copied, values)
		dst[key] = copied
	}
	w.headerSnap = dst
}

func (w *captureWriter) tee(p []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.checked {
		w.checked = true
		// 首字节已写出,header 已提交:此刻拷贝即实际发送的 header。
		// 首次写入与 header 设置在同一调用栈/同一互斥序内,无并发写者。
		w.copyHeaderLocked()
		if w.statusCode == 0 {
			w.statusCode = w.ResponseWriter.Status()
		}
		contentType := ""
		if ct := w.headerSnap["Content-Type"]; len(ct) > 0 {
			contentType = ct[0]
		}
		if contentType != "" && !isTextContentType(contentType) {
			w.skip = true
		}
	}
	if w.buf == nil {
		return // snapshot 之后迟到的写入,直接忽略
	}
	if w.skip || w.truncated {
		return
	}
	remaining := w.limit - w.captured
	if remaining <= 0 {
		w.truncated = true
		return
	}
	if len(p) > remaining {
		w.buf.Write(p[:remaining])
		w.captured += remaining
		w.truncated = true
		return
	}
	w.buf.Write(p)
	w.captured += len(p)
}

func (w *captureWriter) WriteHeader(code int) {
	w.mu.Lock()
	if code > 0 {
		w.statusCode = code
	}
	w.mu.Unlock()
	w.ResponseWriter.WriteHeader(code)
}

func (w *captureWriter) Write(p []byte) (int, error) {
	// 先写真实 writer(业务优先),再 tee 进捕获 buffer
	n, err := w.ResponseWriter.Write(p)
	if n > 0 {
		w.tee(p[:n])
	}
	return n, err
}

func (w *captureWriter) WriteString(s string) (int, error) {
	n, err := w.ResponseWriter.WriteString(s)
	if n > 0 {
		w.tee([]byte(s[:n]))
	}
	return n, err
}

// snapshot 在锁内取出捕获内容、状态码与 header 快照并切断 buffer。
// 之后任何迟到的流式写入只会被 tee 忽略;本方法返回后调用方
// 不得再读 cw.Header()/cw.Status()(与迟到 Render 的 map 写竞争)。
func (w *captureWriter) snapshot() (body string, truncated bool, status int, headers map[string][]string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf != nil {
		body = sanitizeText(w.buf.Bytes())
		w.buf = nil
	}
	// headerSnap 为 nil(零字节写出: 204/304/Hijack/尚未产出首块的慢流)时
	// 绝不能在此 range 活 header map 兜底 —— 迟到的 c.Render 首写会无锁
	// 写入同一 map(WriteContentType),concurrent map range+write 是
	// runtime.throw,recover 兜不住。宁可让这类记录的响应头为空。
	// (c.JSON/错误响应都会经过 Write 设好 headerSnap,实际只有真正
	// 零 body 的响应受影响。)
	status = w.statusCode
	if status == 0 {
		// int 读不会触发 map 级 throw;迟到写状态码的窗口极小且无害
		status = w.ResponseWriter.Status()
	}
	return body, w.truncated, status, w.headerSnap
}

// RelayRecord 记录 relay 请求/响应全量数据的中间件。
// 通过 RELAY_RECORD_ENABLED 环境变量开启,默认关闭(返回 no-op,零开销)。
// 这是本功能唯一的配置项。
//
// 注意:env 必须在本工厂函数内读取而非包 init() —— godotenv.Load(".env")
// 在 main.go InitResources() 中执行,晚于所有包 init();路由注册(调用本
// 函数)发生在 InitResources 之后,此时读取才能拿到 .env 中的值。
func RelayRecord() gin.HandlerFunc {
	if !common.GetEnvOrDefaultBool("RELAY_RECORD_ENABLED", false) {
		return func(c *gin.Context) {
			c.Next()
		}
	}
	maxBody := relayRecordMaxBodyBytes
	return func(c *gin.Context) {
		start := time.Now()
		cw := &captureWriter{
			ResponseWriter: c.Writer,
			buf:            &bytes.Buffer{},
			limit:          maxBody,
		}
		c.Writer = cw

		c.Next()

		// 此时响应已发送完毕,收集逻辑的任何异常都不能影响请求,
		// 整体 recover 兜底;c.Writer 不恢复,包装器对外层完全透明。
		func() {
			defer func() {
				if r := recover(); r != nil {
					common.SysError(fmt.Sprintf("relay record collect panic: %v", r))
				}
			}()
			collectAndEnqueue(c, cw, start, maxBody)
		}()
	}
}

// collectAndEnqueue 在请求 goroutine 内同步物化记录后非阻塞入队。
func collectAndEnqueue(c *gin.Context, cw *captureWriter, start time.Time, maxBody int) {
	record := buildRelayRecord(c, cw, start, maxBody)
	if model.EnqueueRelayRecord(record) {
		if dropped := atomic.AddInt64(&relayRecordDropCount, 1); dropped%1000 == 1 {
			common.SysError(fmt.Sprintf("relay record queue full, total dropped: %d", dropped))
		}
	}
}

// buildRelayRecord 把请求上下文物化为纯数据记录。
// *gin.Context 会被 gin 复用,绝不能传给后台 goroutine —— 返回的
// 结构体只含值类型与 string。状态码与响应头一律取自 snapshot 的
// 锁内快照,绝不读活的 cw.Status()/cw.Header()(见 captureWriter 注释)。
func buildRelayRecord(c *gin.Context, cw *captureWriter, start time.Time, maxBody int) *model.RelayRecord {
	responseBody, responseTruncated, statusCode, responseHeaders := cw.snapshot()
	requestBody, requestTruncated := readCachedRequestBody(c, maxBody)

	// varchar 列必须消毒并裁剪到列宽:Path 来自 c.Request.URL.Path,
	// net/http 会把 %00 解码成裸 NUL、长度也不受控——任一字段非法都会
	// 让整批 INSERT 回滚,殃及同批其他记录。
	return &model.RelayRecord{
		CreatedAt:         common.GetTimestamp(),
		RequestId:         clampColumn(c.GetString(common.RequestIdKey), 64),
		UserId:            common.GetContextKeyInt(c, constant.ContextKeyUserId),
		Username:          clampColumn(common.GetContextKeyString(c, constant.ContextKeyUserName), 64),
		TokenId:           common.GetContextKeyInt(c, constant.ContextKeyTokenId),
		ChannelId:         common.GetContextKeyInt(c, constant.ContextKeyChannelId),
		ModelName:         clampColumn(common.GetContextKeyString(c, constant.ContextKeyOriginalModel), 191),
		Group:             clampColumn(common.GetContextKeyString(c, constant.ContextKeyUsingGroup), 64),
		Ip:                clampColumn(c.ClientIP(), 64),
		Method:            clampColumn(c.Request.Method, 16),
		Path:              clampColumn(c.Request.URL.Path, 255),
		Query:             sanitizeText([]byte(redactQuery(c.Request.URL.RawQuery))),
		RequestHeaders:    marshalHeaders(c.Request.Header),
		RequestBody:       requestBody,
		StatusCode:        statusCode,
		ResponseHeaders:   marshalHeaders(responseHeaders),
		ResponseBody:      responseBody,
		IsStream:          common.GetContextKeyBool(c, constant.ContextKeyIsStream),
		DurationMs:        time.Since(start).Milliseconds(),
		RequestTruncated:  requestTruncated,
		ResponseTruncated: responseTruncated,
	}
}

// readCachedRequestBody 只读取已缓存的请求体(Distribute 等中间件已通过
// common.GetBodyStorage 缓存),绝不主动读 c.Request.Body。
// 磁盘存储限量读取,避免把超大文件整体载入内存。
func readCachedRequestBody(c *gin.Context, maxBody int) (string, bool) {
	value, exists := c.Get(common.KeyBodyStorage)
	if !exists || value == nil {
		return "", false
	}
	storage, ok := value.(common.BodyStorage)
	if !ok {
		return "", false
	}
	size := storage.Size()
	if size <= 0 {
		return "", false
	}
	limit := int64(maxBody)
	truncated := size > limit
	readN := size
	if truncated {
		readN = limit
	}
	if _, err := storage.Seek(0, io.SeekStart); err != nil {
		// I/O 错误不能静默吞掉:请求体字段会变空,运维需要能区分
		// "请求无 body" 和 "读取失败"
		common.SysError("relay record: failed to seek body storage: " + err.Error())
		return "", false
	}
	buf := make([]byte, readN)
	n, _ := io.ReadFull(storage, buf)
	_, _ = storage.Seek(0, io.SeekStart) // 复位,防御后续读取
	if n <= 0 {
		return "", false
	}
	return sanitizeText(buf[:n]), truncated
}

// clampColumn 为 varchar 列消毒并裁剪:去 NUL/无效 UTF-8(防整批
// INSERT 失败),并按字节上限截断(字节数 >= 字符数,必然适配
// varchar(n);截断处的不完整字符由 sanitizeText 修剪)。
func clampColumn(s string, maxBytes int) string {
	if len(s) > maxBytes {
		s = s[:maxBytes]
	}
	out := sanitizeText([]byte(s))
	if len(out) > maxBytes {
		// "[binary data: N bytes]" 占位符本身可能超过窄列(如 varchar(16)),
		// 占位符是纯 ASCII,硬截安全
		out = out[:maxBytes]
	}
	return out
}

func isTextContentType(contentType string) bool {
	contentType = strings.ToLower(contentType)
	return strings.Contains(contentType, "json") ||
		strings.Contains(contentType, "text/") ||
		strings.Contains(contentType, "event-stream") ||
		strings.Contains(contentType, "xml") ||
		strings.Contains(contentType, "x-www-form-urlencoded")
}

// sanitizeText 保证入库内容为合法 UTF-8 且不含 NUL:
// PostgreSQL TEXT 拒绝 0x00/无效 UTF-8,MySQL utf8mb4 同样报错,
// 否则会导致整批插入失败。
// 截断可能切在多字节字符(如中文)中间,此时修剪尾部不完整的
// rune 保留可读前缀,而不是把整段文本替换为占位符。
func sanitizeText(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if bytes.IndexByte(b, 0x00) >= 0 {
		return "[binary data: " + strconv.Itoa(len(b)) + " bytes]"
	}
	if !utf8.Valid(b) {
		trimmed := trimIncompleteTrailingRune(b)
		if len(trimmed) > 0 && utf8.Valid(trimmed) {
			return string(trimmed)
		}
		return "[binary data: " + strconv.Itoa(len(b)) + " bytes]"
	}
	return string(b)
}

// trimIncompleteTrailingRune 去掉末尾被截断的不完整 UTF-8 序列
// (单个字符最长 4 字节,最多回退 3 字节)。
func trimIncompleteTrailingRune(b []byte) []byte {
	for i := 0; i < utf8.UTFMax-1 && len(b) > 0; i++ {
		r, size := utf8.DecodeLastRune(b)
		if r != utf8.RuneError || size != 1 {
			break // 末尾已是完整 rune
		}
		b = b[:len(b)-1]
	}
	return b
}

var relayRecordSensitiveHeaders = map[string]bool{
	"authorization":       true,
	"x-api-key":           true,
	"api-key":             true,
	"x-goog-api-key":      true,
	"cookie":              true,
	"set-cookie":          true,
	"proxy-authorization": true,
}

// marshalHeaders 复制并脱敏请求/响应头后序列化为 JSON。
// http.Header 是活引用,必须先复制再入队。
func marshalHeaders(header map[string][]string) string {
	redacted := make(map[string]string, len(header))
	for key, values := range header {
		if relayRecordSensitiveHeaders[strings.ToLower(key)] {
			redacted[key] = "[REDACTED]"
			continue
		}
		redacted[key] = strings.Join(values, ", ")
	}
	data, err := common.Marshal(redacted)
	if err != nil {
		return ""
	}
	return string(data)
}

// redactQuery 脱敏 query 中的认证参数(Gemini 支持 ?key=API_KEY)。
func redactQuery(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return rawQuery
	}
	changed := false
	for _, name := range []string{"key", "api_key", "access_token"} {
		if values.Has(name) {
			values.Set(name, "[REDACTED]")
			changed = true
		}
	}
	if !changed {
		return rawQuery
	}
	return values.Encode()
}
