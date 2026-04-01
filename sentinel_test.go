package sabi_redis_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/sttk/errs"
	"github.com/sttk/sabi"
	"github.com/sttk/sabi_redis"
)

type /* error reasons */ (
	FailToGetValueSentinel struct{}
	FailToSetValueSentinel struct{}
	FailToDelValueSentinel struct{}
)

///

type RedisSentinelSampleDataAcc struct {
	sabi.DataAcc
}

func (da *RedisSentinelSampleDataAcc) GetSampleKey() (string, errs.Err) {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisSentinelDataConn](da, "redis")
	if err.IsNotOk() {
		return "", err
	}
	redisConn := dc.GetConnection()
	val, e := redisConn.Get(ctx, "sample/sentinel").Result()
	if e != nil {
		if e == redis.Nil {
			return "", errs.Ok()
		}
		return "", errs.New(FailToGetValueSentinel{}, e)
	}
	return val, errs.Ok()
}

func (da *RedisSentinelSampleDataAcc) SetSampleKey(val string) errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisSentinelDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}
	redisConn := dc.GetConnection()
	e := redisConn.Set(ctx, "sample/sentinel", val, 0).Err()
	if e != nil {
		return errs.New(FailToSetValueSentinel{}, e)
	}
	return errs.Ok()
}

func (da *RedisSentinelSampleDataAcc) DelSampleKey() errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisSentinelDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}
	redisConn := dc.GetConnection()
	e := redisConn.Del(ctx, "sample/sentinel").Err()
	if e != nil {
		return errs.New(FailToDelValueSentinel{}, e)
	}
	return errs.Ok()
}

func (da *RedisSentinelSampleDataAcc) SetSampleKeyWithForceBack(val string) errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisSentinelDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}
	redisConn := dc.GetConnection()

	e := redisConn.Set(ctx, "sample_force_back/sentinel", val, 0).Err()
	if e != nil {
		return errs.New(FailToSetValueSentinel{}, e)
	}

	dc.AddForceBack(func(redisConn *redis.Conn) errs.Err {
		e := redisConn.Del(ctx, "sample_force_back/sentinel").Err()
		if e != nil {
			return errs.New("fail to force back", e)
		}
		return errs.Ok()
	})

	e = redisConn.Set(ctx, "sample_force_back_2/sentinel", val, 0).Err()
	if e != nil {
		return errs.New(FailToSetValueSentinel{}, e)
	}

	dc.AddForceBack(func(redisConn *redis.Conn) errs.Err {
		e := redisConn.Del(ctx, "sample_force_back_2/sentinel").Err()
		if e != nil {
			return errs.New("fail to force back", e)
		}
		return errs.Ok()
	})

	return errs.Ok()
}

func (da *RedisSentinelSampleDataAcc) SetSampleKeyWithPreCommit(val string) errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisSentinelDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}

	dc.AddPreCommit(func(redisConn *redis.Conn) errs.Err {
		e := redisConn.Set(ctx, "sample_pre_commit/sentinel", val, 0).Err()
		if e != nil {
			return errs.New(FailToSetValueSentinel{}, e)
		}
		return errs.Ok()
	})

	return errs.Ok()
}

func (da *RedisSentinelSampleDataAcc) SetSampleKeyWithPostCommit(val string) errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisSentinelDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}

	dc.AddPostCommit(func(redisConn *redis.Conn) errs.Err {
		e := redisConn.Set(ctx, "sample_post_commit/sentinel", val, 0).Err()
		if e != nil {
			return errs.New(FailToSetValueSentinel{}, e)
		}
		return errs.Ok()
	})

	return errs.Ok()
}

///

type SampleDataSentinel interface {
	GetSampleKey() (string, errs.Err)
	SetSampleKey(val string) errs.Err
	DelSampleKey() errs.Err
	SetSampleKeyWithForceBack(val string) errs.Err
	SetSampleKeyWithPreCommit(val string) errs.Err
	SetSampleKeyWithPostCommit(val string) errs.Err
}

func sampleLogicSentinel(data SampleDataSentinel) errs.Err {
	val, err := data.GetSampleKey()
	if err.IsNotOk() {
		return err
	}
	if len(val) > 0 {
		panic("val is not empty")
	}

	err = data.SetSampleKey("Hello")
	if err.IsNotOk() {
		return err
	}

	val, err = data.GetSampleKey()
	if err.IsNotOk() {
		return err
	}
	if val != "Hello" {
		panic("val is not \"Hello\"")
	}

	err = data.DelSampleKey()
	if err.IsNotOk() {
		return err
	}

	return errs.Ok()
}

func sampleLogicSentinelWithForceBackOk(data SampleDataSentinel) errs.Err {
	return data.SetSampleKeyWithForceBack("Good Morning")
}

func sampleLogicSentinelWithForceBackErr(data SampleDataSentinel) errs.Err {
	data.SetSampleKeyWithForceBack("Good Afternoon")
	return errs.New("XXX")
}

func sampleLogicSentinelWithPreCommit(data SampleDataSentinel) errs.Err {
	return data.SetSampleKeyWithPreCommit("Good Evening")
}

func sampleLogicSentinelWithPostCommit(data SampleDataSentinel) errs.Err {
	return data.SetSampleKeyWithPostCommit("Good Night")
}

type SampleDataHubSentinel struct {
	sabi.DataHub
	*RedisSentinelSampleDataAcc
}

func NewSampleDataHubSentinel() sabi.DataHub {
	hub := sabi.NewDataHub()
	return SampleDataHubSentinel{
		DataHub:                    hub,
		RedisSentinelSampleDataAcc: &RedisSentinelSampleDataAcc{DataAcc: hub},
	}
}

var _ SampleDataSentinel = (*SampleDataHubSentinel)(nil)

///

func TestSentinel(t *testing.T) {
	t.Run("test NewRedisSentinelDataSrc", func(t *testing.T) {
		ctx := context.Background()
		data := NewSampleDataHubSentinel()
		data.Uses("redis", sabi_redis.NewRedisSentinelDataSrc(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: []string{"127.0.0.1:26479", "127.0.0.1:26480", "127.0.0.1:26481"},
			DB:            0,
		}))
		err := sabi.Run(data, ctx, sampleLogicSentinel)
		assert.True(t, err.IsOk())
	})

	t.Run("fail due to invalid addr", func(t *testing.T) {
		ctx := context.Background()
		data := NewSampleDataHubSentinel()
		data.Uses("redis", sabi_redis.NewRedisSentinelDataSrc(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: []string{"xxxx", "yyyy", "zzzz"},
			DB:            0,
		}))
		err := sabi.Run(data, ctx, sampleLogicSentinel)
		switch r := err.Reason().(type) {
		case sabi.FailToSetupLocalDataSrcs:
			assert.Len(t, r.Errors, 1)
			err2 := r.Errors["redis"]
			switch r2 := err2.Reason().(type) {
			case sabi_redis.RedisSentinelDataSrcFailToPing:
				assert.Equal(t, r2.Options.Addr, "FailoverClient")
				assert.Equal(t, r2.Options.DB, 0)
			default:
				assert.Fail(t, err2.Error())
			}
			assert.Equal(t, err2.Cause().Error(), "redis: all sentinels specified in configuration are unreachable: context deadline exceeded\ncontext deadline exceeded\ncontext deadline exceeded")
		default:
			assert.Fail(t, err.Error())
		}
	})

	t.Run("test Txn and ForceBack", func(t *testing.T) {
		data := NewSampleDataHubSentinel()
		data.Uses("redis", sabi_redis.NewRedisSentinelDataSrc(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: []string{"127.0.0.1:26479", "127.0.0.1:26480", "127.0.0.1:26481"},
			DB:            3,
		}))

		ctx := context.Background()

		err := sabi.Txn(data, ctx, sampleLogicSentinelWithForceBackOk)
		assert.True(t, err.IsOk())

		rdb := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: []string{"127.0.0.1:26479", "127.0.0.1:26480", "127.0.0.1:26481"},
			DB:            3,
		})

		s, e := rdb.Get(ctx, "sample_force_back/sentinel").Result()
		assert.Nil(t, e)
		e = rdb.Del(ctx, "sample_force_back/sentinel").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "Good Morning")

		s, e = rdb.Get(ctx, "sample_force_back_2/sentinel").Result()
		assert.Nil(t, e)
		e = rdb.Del(ctx, "sample_force_back_2/sentinel").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "Good Morning")

		err = sabi.Txn(data, ctx, sampleLogicWithForceBackErr)
		assert.Equal(t, err.Reason(), "XXX")

		s, e = rdb.Get(ctx, "sample_force_back/sentinel").Result()
		assert.Equal(t, e, redis.Nil)
		e = rdb.Del(ctx, "sample_force_back/sentinel").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "")

		s, e = rdb.Get(ctx, "sample_force_back_2/sentinel").Result()
		assert.Equal(t, e, redis.Nil)
		e = rdb.Del(ctx, "sample_force_back_2/sentinel").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "")
	})

	t.Run("test Txn and PreCommit", func(t *testing.T) {
		data := NewSampleDataHubSentinel()
		data.Uses("redis", sabi_redis.NewRedisSentinelDataSrc(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: []string{"127.0.0.1:26479", "127.0.0.1:26480", "127.0.0.1:26481"},
			DB:            4,
		}))

		ctx := context.Background()

		err := sabi.Txn(data, ctx, sampleLogicSentinelWithPreCommit)
		assert.True(t, err.IsOk())

		rdb := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: []string{"127.0.0.1:26479", "127.0.0.1:26480", "127.0.0.1:26481"},
			DB:            4,
		})

		s, e := rdb.Get(ctx, "sample_pre_commit/sentinel").Result()
		assert.Nil(t, e)
		e = rdb.Del(ctx, "sample_pre_commit/sentinel").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "Good Evening")
	})

	t.Run("test Txn and PostCommit", func(t *testing.T) {
		data := NewSampleDataHubSentinel()
		data.Uses("redis", sabi_redis.NewRedisSentinelDataSrc(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: []string{"127.0.0.1:26479", "127.0.0.1:26480", "127.0.0.1:26481"},
			DB:            5,
		}))

		ctx := context.Background()

		err := sabi.Txn(data, ctx, sampleLogicSentinelWithPostCommit)
		assert.True(t, err.IsOk())

		rdb := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: []string{"127.0.0.1:26479", "127.0.0.1:26480", "127.0.0.1:26481"},
			DB:            5,
		})

		s, e := rdb.Get(ctx, "sample_post_commit/sentinel").Result()
		assert.Nil(t, e)
		e = rdb.Del(ctx, "sample_post_commit/sentinel").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "Good Night")
	})
}
