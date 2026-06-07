package model

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"

	"github.com/bytedance/gopkg/util/gopool"
	"github.com/go-redis/redis/v8"
)

// RelayRecord 记录 relay 路由(/v1、/v1beta)的完整请求/响应数据,
// 由 middleware.RelayRecord 采集、本文件的后台消费者异步批量写入 LOG_DB。
//
// 大文本字段(Query/RequestHeaders/RequestBody/ResponseHeaders/ResponseBody)
// 刻意不加任何 gorm tag —— 与 Log.Content/Log.Other 同模式:
// MySQL 驱动默认 longtext,PG/SQLite 默认 text,保证跨库兼容且容量足够。
// 警告: 这些字段也绝不能加 index 或 default tag,否则 MySQL 驱动会把
// 无 size 的 string 强制降为 varchar(191),导致超长内容静默截断。
//
// ModelName 索引列宽必须 <= 191: utf8mb4 下 191*4=764 字节,低于 InnoDB
// COMPACT 行格式的 767 字节索引上限(MySQL 5.7.8 底线);varchar(255)
// 的 1020 字节会让 AutoMigrate 建索引失败、服务无法启动。
type RelayRecord struct {
	Id                int64  `json:"id" gorm:"primaryKey"`
	CreatedAt         int64  `json:"created_at" gorm:"bigint;index:idx_rr_created_at"`
	RequestId         string `json:"request_id" gorm:"type:varchar(64);index:idx_rr_request_id;default:''"`
	UserId            int    `json:"user_id" gorm:"index;default:0"`
	Username          string `json:"username" gorm:"type:varchar(64);default:''"`
	TokenId           int    `json:"token_id" gorm:"default:0"`
	ChannelId         int    `json:"channel_id" gorm:"default:0"`
	ModelName         string `json:"model_name" gorm:"type:varchar(191);index:idx_rr_model;default:''"`
	Group             string `json:"group" gorm:"type:varchar(64);default:''"`
	Ip                string `json:"ip" gorm:"type:varchar(64);default:''"`
	Method            string `json:"method" gorm:"type:varchar(16);default:''"`
	Path              string `json:"path" gorm:"type:varchar(255);default:''"`
	Query             string `json:"query"`
	RequestHeaders    string `json:"request_headers"`
	RequestBody       string `json:"request_body"`
	StatusCode        int    `json:"status_code" gorm:"default:0"`
	ResponseHeaders   string `json:"response_headers"`
	ResponseBody      string `json:"response_body"`
	IsStream          bool   `json:"is_stream"`
	DurationMs        int64  `json:"duration_ms" gorm:"default:0"`
	RequestTruncated  bool   `json:"request_truncated"`
	ResponseTruncated bool   `json:"response_truncated"`
}

func (RelayRecord) TableName() string {
	return "relay_records"
}

const (
	relayRecordStreamKey   = "relay_record_stream"
	relayRecordStreamGroup = "relay_record_group"
	relayRecordStreamField = "d"
)

// 写入流水线参数。固定值,经过验证的安全默认:
//   - 批 50 条 / 单条 INSERT 3MB 预算(< MySQL 5.7 max_allowed_packet 4MB)/ 3 秒必 flush
//   - 队列 5000 条、Stream 上限 10000 条:字节上界 ≈ 条数 × 2MB(请求+响应体各 1MB 上限),
//     正常载荷(每条几十 KB)下为数百 MB
// 声明为 var 仅为测试可注入,运行期不会修改。
var (
	relayRecordChan chan *RelayRecord

	relayRecordQueueSize    = 5000
	relayRecordBatchCount   = 50
	relayRecordBatchBytes   = 3 << 20
	relayRecordFlushSeconds = 3
	relayRecordStreamMaxLen = int64(10000)
)

// InitRelayRecordConsumer 启动后台写入流水线。
// 仅在 RELAY_RECORD_ENABLED=true 时由 main.go 调用,且必须在
// InitLogDB 和 InitRedisClient 之后(.env 已加载、RDB 已就绪)。
//
// 两种模式:
//   - Redis 可用: 请求 goroutine -> 内存 channel -> dispatcher 批量 XADD 到
//     Redis Stream(MAXLEN ~ 裁剪防爆) -> 消费组 XREADGROUP(NOACK)批量读出
//     -> CreateInBatches 写 LOG_DB。多节点共用一个 Stream/消费组,自动分摊;
//     进程崩溃时未读出的数据保留在 Stream 中不丢失。
//   - Redis 不可用: 请求 goroutine -> 内存 channel -> 攒批直写 LOG_DB。
//
// 请求路径永远只接触非阻塞的内存 channel,Redis/DB 故障均不影响业务。
func InitRelayRecordConsumer() {
	queueSize := relayRecordQueueSize

	// 表自检:LOG_SQL_DSN 指向独立库时仅 master 节点执行迁移,slave 可能
	// 先于建表启动。Redis 模式下绝不能让无表节点加入消费组——NOACK 会把
	// 消息领走后写库失败,等于永久销毁其他节点本可落库的记录。
	tableReady := LOG_DB.Migrator().HasTable(&RelayRecord{})
	if !tableReady {
		common.SysError("relay record table relay_records does not exist (master not migrated yet?)")
	}

	if common.RedisEnabled {
		relayRecordChan = make(chan *RelayRecord, queueSize)
		gopool.Go(relayRecordRedisDispatchLoop)
		if tableReady {
			gopool.Go(relayRecordRedisConsumeLoop)
			common.SysLog(fmt.Sprintf("relay record pipeline started (redis stream mode), queue size: %d, stream maxlen: %d", queueSize, relayRecordStreamMaxLen))
		} else {
			common.SysError("relay record local stream consumer disabled (no table); records will be pushed to redis stream for other nodes to consume")
		}
	} else {
		if !tableReady {
			// 无 Redis 又无表:不建 channel,EnqueueRelayRecord 全部丢弃
			return
		}
		relayRecordChan = make(chan *RelayRecord, queueSize)
		gopool.Go(relayRecordMemoryConsumeLoop)
		common.SysLog(fmt.Sprintf("relay record pipeline started (memory mode), queue size: %d", queueSize))
	}
}

// EnqueueRelayRecord 非阻塞入队,绝不阻塞请求路径。
// 队列满或消费者未初始化时直接丢弃,返回 true 表示已丢弃。
func EnqueueRelayRecord(record *RelayRecord) (dropped bool) {
	if relayRecordChan == nil {
		return true
	}
	select {
	case relayRecordChan <- record:
		return false
	default:
		return true
	}
}

func relayRecordSize(record *RelayRecord) int {
	return len(record.RequestBody) + len(record.ResponseBody) +
		len(record.RequestHeaders) + len(record.ResponseHeaders) + len(record.Query)
}

// flushRelayRecordBatch 批量写库,内部按字节预算切批:
// GORM 的 CreateInBatches 只按条数切片,单条 INSERT 字节数不受控,
// 50 条大记录会拼出远超 MySQL max_allowed_packet 的 SQL 导致整批失败。
// 此处是内存/Redis 两条消费路径的汇流点,在这里切批一处覆盖两路。
// 写失败仅记录日志并丢弃该段,绝不重试堆积,避免 DB 故障时雪崩。
func flushRelayRecordBatch(batch []*RelayRecord) {
	start := 0
	accBytes := 0
	for i, record := range batch {
		size := relayRecordSize(record)
		// 加入当前记录会超预算时,先把之前积累的写出
		// (单条自身超预算则独立成批,单行 INSERT 不会死循环)
		if i > start && accBytes+size > relayRecordBatchBytes {
			writeRelayRecordChunk(batch[start:i])
			start = i
			accBytes = 0
		}
		accBytes += size
	}
	writeRelayRecordChunk(batch[start:])
}

// writeRelayRecordChunk 写出一段记录。日志附首条 request_id,
// 便于定位毒化整段的记录。
func writeRelayRecordChunk(chunk []*RelayRecord) {
	if len(chunk) == 0 {
		return
	}
	if err := LOG_DB.CreateInBatches(chunk, relayRecordBatchCount).Error; err != nil {
		common.SysError(fmt.Sprintf("failed to write relay records (dropped %d rows, first request_id: %s): %s",
			len(chunk), chunk[0].RequestId, err.Error()))
	}
}

// relayRecordMemoryConsumeLoop 无 Redis 时的回退模式:内存攒批直写 DB。
// 这里的字节累计只是提前 flush 的优化;真正保证单条 INSERT 不超
// max_allowed_packet 的切批在 flushRelayRecordBatch 内完成。
func relayRecordMemoryConsumeLoop() {
	defer func() {
		if r := recover(); r != nil {
			common.SysError(fmt.Sprintf("relay record memory consumer panic, restarting: %v", r))
			time.Sleep(time.Second) // 防 panic 风暴空转
			gopool.Go(relayRecordMemoryConsumeLoop)
		}
	}()

	ticker := time.NewTicker(time.Duration(relayRecordFlushSeconds) * time.Second)
	defer ticker.Stop()

	batch := make([]*RelayRecord, 0, relayRecordBatchCount)
	batchBytes := 0

	flush := func() {
		flushRelayRecordBatch(batch)
		batch = batch[:0]
		batchBytes = 0
	}

	for {
		select {
		case record := <-relayRecordChan:
			batch = append(batch, record)
			batchBytes += relayRecordSize(record)
			if len(batch) >= relayRecordBatchCount || batchBytes >= relayRecordBatchBytes {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

var (
	relayRecordEncodeFailures int64
	relayRecordDecodeFailures int64
)

// relayRecordRedisDispatchLoop 把内存 channel 中的记录批量(pipeline)
// XADD 到 Redis Stream。XADD 失败仅记录日志并丢弃,不回退不重试。
func relayRecordRedisDispatchLoop() {
	defer func() {
		if r := recover(); r != nil {
			common.SysError(fmt.Sprintf("relay record redis dispatcher panic, restarting: %v", r))
			time.Sleep(time.Second) // 防 panic 风暴空转
			gopool.Go(relayRecordRedisDispatchLoop)
		}
	}()

	ctx := context.Background()
	for {
		record := <-relayRecordChan
		pending := []*RelayRecord{record}
		// 非阻塞地把 channel 中已有的记录一并取出,合并为一次 pipeline
	drain:
		for len(pending) < relayRecordBatchCount {
			select {
			case r := <-relayRecordChan:
				pending = append(pending, r)
			default:
				break drain
			}
		}

		pipe := common.RDB.Pipeline()
		for _, r := range pending {
			data, err := common.Marshal(r)
			if err != nil {
				if n := atomic.AddInt64(&relayRecordEncodeFailures, 1); n%1000 == 1 {
					common.SysError(fmt.Sprintf("relay record marshal failed (total dropped: %d): %s", n, err.Error()))
				}
				continue
			}
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: relayRecordStreamKey,
				MaxLen: relayRecordStreamMaxLen,
				Approx: true,
				Values: map[string]interface{}{relayRecordStreamField: string(data)},
			})
		}
		if _, err := pipe.Exec(ctx); err != nil {
			common.SysError(fmt.Sprintf("failed to push relay records to redis stream (dropped %d rows): %s", len(pending), err.Error()))
		}
	}
}

// relayRecordRedisConsumeLoop 通过消费组从 Redis Stream 批量读出并写库。
// 使用 NOACK:进程崩溃最多丢失"已读出未落库"的一批(<=BATCH_COUNT 条),
// 换取无需 XACK/XPENDING/XCLAIM 回收逻辑;未读出的数据保留在 Stream 中。
func relayRecordRedisConsumeLoop() {
	defer func() {
		if r := recover(); r != nil {
			common.SysError(fmt.Sprintf("relay record redis consumer panic, restarting: %v", r))
			time.Sleep(time.Second) // 防 panic 风暴空转
			gopool.Go(relayRecordRedisConsumeLoop)
		}
	}()

	ctx := context.Background()

	// 创建消费组(幂等):已存在时返回 BUSYGROUP,忽略即可。
	// 起始 ID 用 "0" 以消费 Stream 中已有的遗留数据。
	if err := common.RDB.XGroupCreateMkStream(ctx, relayRecordStreamKey, relayRecordStreamGroup, "0").Err(); err != nil &&
		!strings.Contains(err.Error(), "BUSYGROUP") {
		common.SysError("failed to create relay record stream group: " + err.Error())
	}

	hostname, _ := os.Hostname()
	consumerName := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	lastBacklogCheck := time.Now()

	for {
		// 周期性检查 Stream 积压:XADD 的 MAXLEN ~ 裁剪不区分已读/未读,
		// DB 写入慢导致未读积压触顶时,最老的未消费记录会被静默裁掉,
		// 这里至少让运维能察觉。
		if time.Since(lastBacklogCheck) >= time.Minute {
			lastBacklogCheck = time.Now()
			if length, err := common.RDB.XLen(ctx, relayRecordStreamKey).Result(); err == nil &&
				length >= relayRecordStreamMaxLen*9/10 {
				common.SysError(fmt.Sprintf("relay record stream backlog %d near maxlen %d, oldest unconsumed records are being trimmed", length, relayRecordStreamMaxLen))
			}
		}

		result, err := common.RDB.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    relayRecordStreamGroup,
			Consumer: consumerName,
			Streams:  []string{relayRecordStreamKey, ">"},
			Count:    int64(relayRecordBatchCount),
			Block:    time.Duration(relayRecordFlushSeconds) * time.Second,
			NoAck:    true,
		}).Result()
		if err != nil {
			if err == redis.Nil {
				continue // Block 超时,无新消息
			}
			common.SysError("failed to read relay records from redis stream: " + err.Error())
			time.Sleep(time.Second) // 防错误风暴
			continue
		}

		batch := make([]*RelayRecord, 0, relayRecordBatchCount)
		for _, stream := range result {
			for _, message := range stream.Messages {
				data, ok := message.Values[relayRecordStreamField].(string)
				if !ok {
					if n := atomic.AddInt64(&relayRecordDecodeFailures, 1); n%1000 == 1 {
						common.SysError(fmt.Sprintf("relay record stream message has no payload field (total dropped: %d)", n))
					}
					continue
				}
				var record RelayRecord
				if err := common.UnmarshalJsonStr(data, &record); err != nil {
					if n := atomic.AddInt64(&relayRecordDecodeFailures, 1); n%1000 == 1 {
						common.SysError(fmt.Sprintf("relay record unmarshal failed (total dropped: %d): %s", n, err.Error()))
					}
					continue
				}
				record.Id = 0 // 由数据库自增生成
				batch = append(batch, &record)
			}
		}
		flushRelayRecordBatch(batch)
	}
}
