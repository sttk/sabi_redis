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
	FailToGetValue struct{}
	FailToSetValue struct{}
	FailToDelValue struct{}
)

///

type RedisSampleDataAcc struct {
	sabi.DataAcc
}

func (da *RedisSampleDataAcc) GetSampleKey() (string, errs.Err) {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisDataConn](da, "redis")
	if err.IsNotOk() {
		return "", err
	}
	redisConn := dc.GetConnection()
	val, e := redisConn.Get(ctx, "sample").Result()
	if e != nil {
		if e == redis.Nil {
			return "", errs.Ok()
		}
		return "", errs.New(FailToGetValue{}, e)
	}
	return val, errs.Ok()
}

func (da *RedisSampleDataAcc) SetSampleKey(val string) errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}
	redisConn := dc.GetConnection()
	e := redisConn.Set(ctx, "sample", val, 0).Err()
	if e != nil {
		return errs.New(FailToSetValue{}, e)
	}
	return errs.Ok()
}

func (da *RedisSampleDataAcc) DelSampleKey() errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}
	redisConn := dc.GetConnection()
	e := redisConn.Del(ctx, "sample").Err()
	if e != nil {
		return errs.New(FailToDelValue{}, e)
	}
	return errs.Ok()
}

func (da *RedisSampleDataAcc) SetSampleKeyWithForceBack(val string) errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}
	redisConn := dc.GetConnection()

	e := redisConn.Set(ctx, "sample_force_back", val, 0).Err()
	if e != nil {
		return errs.New(FailToSetValue{}, e)
	}

	dc.AddForceBack(func(redisConn *redis.Conn) errs.Err {
		e := redisConn.Del(ctx, "sample_force_back").Err()
		if e != nil {
			return errs.New("fail to force back", e)
		}
		return errs.Ok()
	})

	e = redisConn.Set(ctx, "sample_force_back_2", val, 0).Err()
	if e != nil {
		return errs.New(FailToSetValue{}, e)
	}

	dc.AddForceBack(func(redisConn *redis.Conn) errs.Err {
		e := redisConn.Del(ctx, "sample_force_back_2").Err()
		if e != nil {
			return errs.New("fail to force back", e)
		}
		return errs.Ok()
	})

	return errs.Ok()
}

func (da *RedisSampleDataAcc) SetSampleKeyWithPreCommit(val string) errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}

	dc.AddPreCommit(func(redisConn *redis.Conn) errs.Err {
		e := redisConn.Set(ctx, "sample_pre_commit", val, 0).Err()
		if e != nil {
			return errs.New(FailToSetValue{}, e)
		}
		return errs.Ok()
	})

	return errs.Ok()
}

func (da *RedisSampleDataAcc) SetSampleKeyWithPostCommit(val string) errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}

	dc.AddPostCommit(func(redisConn *redis.Conn) errs.Err {
		e := redisConn.Set(ctx, "sample_post_commit", val, 0).Err()
		if e != nil {
			return errs.New(FailToSetValue{}, e)
		}
		return errs.Ok()
	})

	return errs.Ok()
}

///

type SampleData interface {
	GetSampleKey() (string, errs.Err)
	SetSampleKey(val string) errs.Err
	DelSampleKey() errs.Err
	SetSampleKeyWithForceBack(val string) errs.Err
	SetSampleKeyWithPreCommit(val string) errs.Err
	SetSampleKeyWithPostCommit(val string) errs.Err
}

func sampleLogic(data SampleData) errs.Err {
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

func sampleLogicWithForceBackOk(data SampleData) errs.Err {
	return data.SetSampleKeyWithForceBack("Good Morning")
}

func sampleLogicWithForceBackErr(data SampleData) errs.Err {
	data.SetSampleKeyWithForceBack("Good Afternoon")
	return errs.New("XXX")
}

func sampleLogicWithPreCommit(data SampleData) errs.Err {
	return data.SetSampleKeyWithPreCommit("Good Evening")
}

func sampleLogicWithPostCommit(data SampleData) errs.Err {
	return data.SetSampleKeyWithPostCommit("Good Night")
}

type SampleDataHub struct {
	sabi.DataHub
	*RedisSampleDataAcc
}

func NewSampleDataHub() sabi.DataHub {
	hub := sabi.NewDataHub()
	return SampleDataHub{
		DataHub:            hub,
		RedisSampleDataAcc: &RedisSampleDataAcc{DataAcc: hub},
	}
}

var _ SampleData = (*SampleDataHub)(nil)

///

func TestStandalone(t *testing.T) {
	t.Run("test NewRedisDataSrc", func(t *testing.T) {
		ctx := context.Background()
		data := NewSampleDataHub()
		data.Uses("redis", sabi_redis.NewRedisDataSrc(&redis.Options{
			Addr: "127.0.0.1:6379",
			DB:   0,
		}))
		err := sabi.Run(data, ctx, sampleLogic)
		assert.True(t, err.IsOk())
	})

	t.Run("fail due to invalid addr", func(t *testing.T) {
		ctx := context.Background()
		data := NewSampleDataHub()
		data.Uses("redis", sabi_redis.NewRedisDataSrc(&redis.Options{
			Addr: "xxxx",
			DB:   0,
		}))
		err := sabi.Run(data, ctx, sampleLogic)
		switch r := err.Reason().(type) {
		case sabi.FailToSetupLocalDataSrcs:
			assert.Len(t, r.Errors, 1)
			err2 := r.Errors["redis"]
			switch r2 := err2.Reason().(type) {
			case sabi_redis.RedisDataSrcFailToPing:
				assert.Equal(t, r2.Options.Addr, "xxxx")
				assert.Equal(t, r2.Options.DB, 0)
			default:
				assert.Fail(t, err2.Error())
			}
			assert.Equal(t, err2.Cause().Error(), "dial tcp: address xxxx: missing port in address")
		default:
			assert.Fail(t, err.Error())
		}
	})

	t.Run("test Txn and ForceBack", func(t *testing.T) {
		data := NewSampleDataHub()
		data.Uses("redis", sabi_redis.NewRedisDataSrc(&redis.Options{
			Addr: "127.0.0.1:6379",
			DB:   3,
		}))

		ctx := context.Background()

		err := sabi.Txn(data, ctx, sampleLogicWithForceBackOk)
		assert.True(t, err.IsOk())

		rdb := redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:6379",
			DB:   3,
		})

		s, e := rdb.Get(ctx, "sample_force_back").Result()
		assert.Nil(t, e)
		e = rdb.Del(ctx, "sample_force_back").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "Good Morning")

		s, e = rdb.Get(ctx, "sample_force_back_2").Result()
		assert.Nil(t, e)
		e = rdb.Del(ctx, "sample_force_back_2").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "Good Morning")

		err = sabi.Txn(data, ctx, sampleLogicWithForceBackErr)
		assert.Equal(t, err.Reason(), "XXX")

		s, e = rdb.Get(ctx, "sample_force_back").Result()
		assert.Equal(t, e, redis.Nil)
		e = rdb.Del(ctx, "sample_force_back").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "")

		s, e = rdb.Get(ctx, "sample_force_back_2").Result()
		assert.Equal(t, e, redis.Nil)
		e = rdb.Del(ctx, "sample_force_back_2").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "")
	})

	t.Run("test Txn and PreCommit", func(t *testing.T) {
		data := NewSampleDataHub()
		data.Uses("redis", sabi_redis.NewRedisDataSrc(&redis.Options{
			Addr: "127.0.0.1:6379",
			DB:   4,
		}))

		ctx := context.Background()

		err := sabi.Txn(data, ctx, sampleLogicWithPreCommit)
		assert.True(t, err.IsOk())

		rdb := redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:6379",
			DB:   4,
		})

		s, e := rdb.Get(ctx, "sample_pre_commit").Result()
		assert.Nil(t, e)
		e = rdb.Del(ctx, "sample_pre_commit").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "Good Evening")
	})

	t.Run("test Txn and PostCommit", func(t *testing.T) {
		data := NewSampleDataHub()
		data.Uses("redis", sabi_redis.NewRedisDataSrc(&redis.Options{
			Addr: "127.0.0.1:6379",
			DB:   5,
		}))

		ctx := context.Background()

		err := sabi.Txn(data, ctx, sampleLogicWithPostCommit)
		assert.True(t, err.IsOk())

		rdb := redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:6379",
			DB:   5,
		})

		s, e := rdb.Get(ctx, "sample_post_commit").Result()
		assert.Nil(t, e)
		e = rdb.Del(ctx, "sample_post_commit").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "Good Night")
	})
}
