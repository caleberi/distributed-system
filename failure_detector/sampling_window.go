package failuredetector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Scorer interface {
	ID() string
	Score() int64
}

type SamplingWindow[T Scorer] struct {
	key  string
	size int
	ttl  time.Duration
	rdb  *redis.Client
}

func NewSamplingWindow[T Scorer](
	key string, size int, ttl time.Duration, opts *redis.Options) (*SamplingWindow[T], error) {
	if size <= 0 {
		size = 10
	}
	if ttl <= 0 {
		ttl = time.Second
	}

	// In NewFailureDetector, after window creation
	ctx := context.Background()
	cache := redis.NewClient(opts)
	if _, err := cache.Ping(ctx).Result(); err != nil {
		return nil, (fmt.Errorf("failed to connect to Redis: %w", err))
	}

	return &SamplingWindow[T]{
		key:  key,
		size: size,
		ttl:  ttl,
		rdb:  redis.NewClient(opts),
	}, nil
}

func (sw *SamplingWindow[T]) clean(ctx context.Context) error {
	windowKey := sw.key + ":main_set"
	expiredWindowKey := sw.key + ":expired_set"
	dataPrefix := sw.key + ":item:"
	card, err := sw.rdb.ZCard(ctx, windowKey).Result()
	if err != nil {
		return err
	}

	if card < int64(sw.size) {
		return nil
	}

	query := &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", time.Now().UnixMilli())}
	expired, err := sw.rdb.ZRangeByScore(ctx, expiredWindowKey, query).Result()
	if err != nil {
		return err
	}
	if len(expired) == 0 {
		return nil
	}

	p := sw.rdb.Pipeline()
	for _, id := range expired {
		p.ZRem(ctx, expiredWindowKey, id)
		p.ZRem(ctx, windowKey, id)
		p.Del(ctx, dataPrefix+id)
	}
	if _, err := p.Exec(ctx); err != nil {
		return err
	}

	card, err = sw.rdb.ZCard(ctx, windowKey).Result()
	if err != nil {
		return err
	}
	if card > int64(sw.size) {
		expired, err := sw.rdb.ZRange(ctx, windowKey, 0, card-int64(sw.size)-1).Result()
		if err != nil {
			return err
		}
		if len(expired) > 0 {
			p = sw.rdb.Pipeline()
			for _, id := range expired {
				p.ZRem(ctx, windowKey, id)
				p.ZRem(ctx, expiredWindowKey, id)
				p.Del(ctx, dataPrefix+id)
			}
			if _, err := p.Exec(ctx); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sw *SamplingWindow[T]) Add(ctx context.Context, entry T) error {
	if err := sw.clean(ctx); err != nil {
		return err
	}
	jsn, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	mainKey := sw.key + ":main_set"
	expKey := sw.key + ":expired_set"
	dataPrefix := sw.key + ":item:"
	member := entry.ID()
	score := float64(entry.Score())
	expScore := float64(time.Now().UnixMilli() + int64(sw.ttl.Milliseconds()))

	p := sw.rdb.Pipeline()
	p.Set(ctx, dataPrefix+member, jsn, 0)
	p.ZAdd(ctx, mainKey, redis.Z{Score: score, Member: member})
	p.ZAdd(ctx, expKey, redis.Z{Score: expScore, Member: member})
	if _, err := p.Exec(ctx); err != nil {
		return err
	}

	card, err := sw.rdb.ZCard(ctx, mainKey).Result()
	if err != nil {
		return err
	}

	if card > int64(sw.size) {
		expired, err := sw.rdb.ZRange(ctx, expKey, 0, card-int64(sw.size)-1).Result()
		if err != nil {
			return err
		}
		if len(expired) > 0 {
			p = sw.rdb.Pipeline()
			for _, id := range expired {
				p.ZRem(ctx, mainKey, id)
				p.ZRem(ctx, expKey, id)
				p.Del(ctx, dataPrefix+id)
			}
			if _, err := p.Exec(ctx); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sw *SamplingWindow[T]) Get(ctx context.Context) ([]T, error) {
	if err := sw.clean(ctx); err != nil {
		return nil, err
	}
	mainKey := sw.key + ":main_set"
	dataPrefix := sw.key + ":item:"
	members, err := sw.rdb.ZRevRange(ctx, mainKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	result := make([]T, 0, len(members))
	for _, member := range members {
		js, err := sw.rdb.Get(ctx, dataPrefix+member).Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			return nil, err
		}
		var entry T
		if err := json.Unmarshal([]byte(js), &entry); err != nil {
			return nil, err
		}
		result = append(result, entry)
	}

	return result, nil
}
