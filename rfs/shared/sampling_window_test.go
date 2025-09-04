package shared

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestSamplingWindowClean(t *testing.T) {
	ctx := context.Background()
	opts := &redis.Options{Addr: "localhost:6379"}
	sw, err := NewSamplingWindow[Entry]("test", 10, 30*time.Second, opts)
	assert.NoError(t, err)

	defer func(t *testing.T) {
		mainKey := sw.key + ":main_set"
		expKey := sw.key + ":expired_set"
		dataPrefix := sw.key + ":item:"
		err := sw.rdb.Del(ctx, mainKey, expKey).Err()
		assert.NoError(t, err)
		keys, err := sw.rdb.Keys(ctx, dataPrefix+"*").Result()
		assert.NoError(t, err)
		assert.NotEmpty(t, keys)
		err = sw.rdb.Del(ctx, keys...).Err()
		assert.NoError(t, err)
	}(t)

	now := time.Now()
	for i := 0; i < 5; i++ { // Underfill window (size=10)
		entry := Entry{
			Id:       uuid.New().String(),
			Eta:      now.Add(time.Duration(-i) * time.Second),
			Duration: time.Second,
		}
		err := sw.Add(ctx, entry)
		assert.NoError(t, err)
	}

	card, _ := sw.rdb.ZCard(ctx, sw.key+":main_set").Result()
	assert.Equal(t, int64(5), card)

	expiredEntry := Entry{Id: uuid.New().String(), Eta: now.Add(-31 * time.Second), Duration: time.Second}
	sw.Add(ctx, expiredEntry)
	card, _ = sw.rdb.ZCard(ctx, sw.key+":main_set").Result()
	assert.Equal(t, int64(6), card)

	for i := 0; i < 6; i++ {
		entry := Entry{
			Id:       uuid.New().String(),
			Eta:      now.Add(time.Duration(-i) * time.Second),
			Duration: time.Second,
		}
		sw.Add(ctx, entry)
	}
	sw.Add(ctx, expiredEntry)
	card, _ = sw.rdb.ZCard(ctx, sw.key+":main_set").Result()
	assert.LessOrEqual(t, card, int64(10))
}
