package model

import (
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestEnqueueRelayRecordNilChannel(t *testing.T) {
	old := relayRecordChan
	t.Cleanup(func() { relayRecordChan = old })

	relayRecordChan = nil
	if dropped := EnqueueRelayRecord(&RelayRecord{}); !dropped {
		t.Fatal("nil channel must drop, not block or panic")
	}
}

func TestEnqueueRelayRecordFullChannelNeverBlocks(t *testing.T) {
	old := relayRecordChan
	t.Cleanup(func() { relayRecordChan = old; relayRecordQueueBytes = 0 })

	relayRecordChan = make(chan *RelayRecord, 1)
	if dropped := EnqueueRelayRecord(&RelayRecord{}); dropped {
		t.Fatal("queue has space, must enqueue")
	}
	// 队列已满:必须立即返回 dropped=true,绝不阻塞(阻塞会令测试超时)
	if dropped := EnqueueRelayRecord(&RelayRecord{}); !dropped {
		t.Fatal("full queue must drop")
	}
	if len(relayRecordChan) != 1 {
		t.Fatalf("queue length = %d, want 1", len(relayRecordChan))
	}
}

// 字节预算是大记录(单条可达 ~32MB)场景下真正的内存上界:
// 超预算必须丢弃,且丢弃/出队都要正确归还预算,不能泄漏计数。
func TestEnqueueRelayRecordByteBudget(t *testing.T) {
	oldChan := relayRecordChan
	oldBudget := relayRecordQueueMaxBytes
	t.Cleanup(func() {
		relayRecordChan = oldChan
		relayRecordQueueMaxBytes = oldBudget
		relayRecordQueueBytes = 0
	})

	relayRecordChan = make(chan *RelayRecord, 100)
	relayRecordQueueMaxBytes = 100
	relayRecordQueueBytes = 0

	big := &RelayRecord{RequestBody: strings.Repeat("a", 80)}
	if dropped := EnqueueRelayRecord(big); dropped {
		t.Fatal("first record within budget must enqueue")
	}
	// 80 + 80 > 100:超预算必须丢弃
	if dropped := EnqueueRelayRecord(big); !dropped {
		t.Fatal("over-budget record must drop")
	}
	// 丢弃后预算应已归还(仍是 80),小记录可继续入队
	small := &RelayRecord{RequestBody: "tiny"}
	if dropped := EnqueueRelayRecord(small); dropped {
		t.Fatal("small record within remaining budget must enqueue")
	}
	// 模拟消费侧出队归还预算后,大记录又可入队
	dequeueRelayRecord(<-relayRecordChan)
	dequeueRelayRecord(<-relayRecordChan)
	if relayRecordQueueBytes != 0 {
		t.Fatalf("queue bytes leaked: %d", relayRecordQueueBytes)
	}
	if dropped := EnqueueRelayRecord(big); dropped {
		t.Fatal("after dequeue, budget must be available again")
	}
}

// flushRelayRecordBatch 的字节切批是防止单条 INSERT 超过 MySQL
// max_allowed_packet 的正确性依赖:切批后必须不丢、不重,
// 单条超预算的记录也必须独立写出而不是死循环或被跳过。
func TestFlushRelayRecordBatchSplitsByBytesWithoutLoss(t *testing.T) {
	oldDB := LOG_DB
	oldCount := relayRecordBatchCount
	oldBytes := relayRecordBatchBytes
	t.Cleanup(func() {
		LOG_DB = oldDB
		relayRecordBatchCount = oldCount
		relayRecordBatchBytes = oldBytes
	})

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&RelayRecord{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	LOG_DB = db
	relayRecordBatchCount = 50
	relayRecordBatchBytes = 100 // 极小预算,强制切批

	batch := []*RelayRecord{
		{RequestId: "r1", RequestBody: strings.Repeat("a", 60)},
		{RequestId: "r2", RequestBody: strings.Repeat("b", 60)},  // 60+60>100,切批点
		{RequestId: "r3", RequestBody: strings.Repeat("c", 300)}, // 单条超预算,独立成批
		{RequestId: "r4", RequestBody: "small"},
		{RequestId: "r5"}, // 空 body
	}
	flushRelayRecordBatch(batch)

	var count int64
	if err := db.Model(&RelayRecord{}).Count(&count).Error; err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != int64(len(batch)) {
		t.Fatalf("rows = %d, want %d (split must not lose or duplicate records)", count, len(batch))
	}
	// 抽查超大单条确实完整落库
	var big RelayRecord
	if err := db.Where("request_id = ?", "r3").First(&big).Error; err != nil {
		t.Fatalf("oversized record missing: %v", err)
	}
	if len(big.RequestBody) != 300 {
		t.Fatalf("oversized record body = %d bytes, want 300", len(big.RequestBody))
	}

	// 空批不应 panic
	flushRelayRecordBatch(nil)
}
