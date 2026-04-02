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
	FailToGetValueCluster struct{}
	FailToSetValueCluster struct{}
	FailToDelValueCluster struct{}
)

///

type RedisClusterSampleDataAcc struct {
	sabi.DataAcc
}

func (da *RedisClusterSampleDataAcc) GetSampleKey() (string, errs.Err) {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisClusterDataConn](da, "redis")
	if err.IsNotOk() {
		return "", err
	}
	redisConn := dc.GetConnection()
	val, e := redisConn.Get(ctx, "sample/cluster").Result()
	if e != nil {
		if e == redis.Nil {
			return "", errs.Ok()
		}
		return "", errs.New(FailToGetValueCluster{}, e)
	}
	return val, errs.Ok()
}

func (da *RedisClusterSampleDataAcc) SetSampleKey(val string) errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisClusterDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}
	redisConn := dc.GetConnection()
	e := redisConn.Set(ctx, "sample/cluster", val, 0).Err()
	if e != nil {
		return errs.New(FailToSetValueCluster{}, e)
	}
	return errs.Ok()
}

func (da *RedisClusterSampleDataAcc) DelSampleKey() errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisClusterDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}
	redisConn := dc.GetConnection()
	e := redisConn.Del(ctx, "sample/cluster").Err()
	if e != nil {
		return errs.New(FailToDelValueCluster{}, e)
	}
	return errs.Ok()
}

func (da *RedisClusterSampleDataAcc) SetSampleKeyWithForceBack(val string) errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisClusterDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}
	redisConn := dc.GetConnection()

	e := redisConn.Set(ctx, "sample_force_back/cluster", val, 0).Err()
	if e != nil {
		return errs.New(FailToSetValueCluster{}, e)
	}

	dc.AddForceBack(func(redisConn *redis.ClusterClient) errs.Err {
		e := redisConn.Del(ctx, "sample_force_back/cluster").Err()
		if e != nil {
			return errs.New("fail to force back", e)
		}
		return errs.Ok()
	})

	e = redisConn.Set(ctx, "sample_force_back_2/cluster", val, 0).Err()
	if e != nil {
		return errs.New(FailToSetValueCluster{}, e)
	}

	dc.AddForceBack(func(redisConn *redis.ClusterClient) errs.Err {
		e := redisConn.Del(ctx, "sample_force_back_2/cluster").Err()
		if e != nil {
			return errs.New("fail to force back", e)
		}
		return errs.Ok()
	})

	return errs.Ok()
}

func (da *RedisClusterSampleDataAcc) SetSampleKeyWithPreCommit(val string) errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisClusterDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}

	dc.AddPreCommit(func(redisConn *redis.ClusterClient) errs.Err {
		e := redisConn.Set(ctx, "sample_pre_commit/cluster", val, 0).Err()
		if e != nil {
			return errs.New(FailToSetValueCluster{}, e)
		}
		return errs.Ok()
	})

	return errs.Ok()
}

func (da *RedisClusterSampleDataAcc) SetSampleKeyWithPostCommit(val string) errs.Err {
	ctx := da.Context()
	dc, err := sabi.GetDataConn[*sabi_redis.RedisClusterDataConn](da, "redis")
	if err.IsNotOk() {
		return err
	}

	dc.AddPostCommit(func(redisConn *redis.ClusterClient) errs.Err {
		e := redisConn.Set(ctx, "sample_post_commit/cluster", val, 0).Err()
		if e != nil {
			return errs.New(FailToSetValueCluster{}, e)
		}
		return errs.Ok()
	})

	return errs.Ok()
}

///

type SampleDataCluster interface {
	GetSampleKey() (string, errs.Err)
	SetSampleKey(val string) errs.Err
	DelSampleKey() errs.Err
	SetSampleKeyWithForceBack(val string) errs.Err
	SetSampleKeyWithPreCommit(val string) errs.Err
	SetSampleKeyWithPostCommit(val string) errs.Err
}

func sampleLogicCluster(data SampleDataCluster) errs.Err {
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

func sampleLogicClusterWithForceBackOk(data SampleDataCluster) errs.Err {
	return data.SetSampleKeyWithForceBack("Good Morning")
}

func sampleLogicClusterWithForceBackErr(data SampleDataCluster) errs.Err {
	data.SetSampleKeyWithForceBack("Good Afternoon")
	return errs.New("XXX")
}

func sampleLogicClusterWithPreCommit(data SampleDataCluster) errs.Err {
	return data.SetSampleKeyWithPreCommit("Good Evening")
}

func sampleLogicClusterWithPostCommit(data SampleDataCluster) errs.Err {
	return data.SetSampleKeyWithPostCommit("Good Night")
}

type SampleDataHubCluster struct {
	sabi.DataHub
	*RedisClusterSampleDataAcc
}

func NewSampleDataHubCluster() sabi.DataHub {
	hub := sabi.NewDataHub()
	return SampleDataHubCluster{
		DataHub:                   hub,
		RedisClusterSampleDataAcc: &RedisClusterSampleDataAcc{DataAcc: hub},
	}
}

var _ SampleDataCluster = (*SampleDataHubCluster)(nil)

///

func TestCluster(t *testing.T) {
	t.Run("test NewRedisClusterDataSrc", func(t *testing.T) {
		ctx := context.Background()
		data := NewSampleDataHubCluster()
		data.Uses("redis", sabi_redis.NewRedisClusterDataSrc(&redis.ClusterOptions{
			Addrs: []string{
				"127.0.0.1:7000",
				"127.0.0.1:7001",
				"127.0.0.1:7002",
			},
		}))
		err := sabi.Run(data, ctx, sampleLogicCluster)
		assert.True(t, err.IsOk())
	})

	t.Run("fail due to invalid addr", func(t *testing.T) {
		ctx := context.Background()
		data := NewSampleDataHubCluster()
		data.Uses("redis", sabi_redis.NewRedisClusterDataSrc(&redis.ClusterOptions{
			Addrs: []string{
				"xxxx",
				"yyyy",
				"zzzz",
			},
		}))
		err := sabi.Run(data, ctx, sampleLogicCluster)
		switch r := err.Reason().(type) {
		case sabi.FailToSetupLocalDataSrcs:
			assert.Len(t, r.Errors, 1)
			err2 := r.Errors["redis"]
			switch r2 := err2.Reason().(type) {
			case sabi_redis.RedisClusterDataSrcFailToPing:
				assert.Equal(t, r2.Options.Addrs, []string{
					"xxxx",
					"yyyy",
					"zzzz",
				})
			default:
				assert.Fail(t, err2.Error())
			}
			assert.Equal(t, err2.Cause().Error(), "dial tcp: address yyyy: missing port in address")
		default:
			assert.Fail(t, err.Error())
		}
	})

	t.Run("test Txn and ForceBack", func(t *testing.T) {
		data := NewSampleDataHubCluster()
		data.Uses("redis", sabi_redis.NewRedisClusterDataSrc(&redis.ClusterOptions{
			Addrs: []string{
				"127.0.0.1:7000",
				"127.0.0.1:7001",
				"127.0.0.1:7002",
			},
		}))

		ctx := context.Background()

		err := sabi.Txn(data, ctx, sampleLogicClusterWithForceBackOk)
		assert.True(t, err.IsOk())

		var e error
		rdb := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{
				"127.0.0.1:7000",
				"127.0.0.1:7001",
				"127.0.0.1:7002",
			},
		})

		s, e := rdb.Get(ctx, "sample_force_back/cluster").Result()
		assert.Nil(t, e)
		e = rdb.Del(ctx, "sample_force_back/cluster").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "Good Morning")

		s, e = rdb.Get(ctx, "sample_force_back_2/cluster").Result()
		assert.Nil(t, e)
		e = rdb.Del(ctx, "sample_force_back_2/cluster").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "Good Morning")

		err = sabi.Txn(data, ctx, sampleLogicClusterWithForceBackErr)
		assert.Equal(t, err.Reason(), "XXX")

		s, e = rdb.Get(ctx, "sample_force_back/cluster").Result()
		assert.Equal(t, e, redis.Nil)
		e = rdb.Del(ctx, "sample_force_back/cluster").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "")

		s, e = rdb.Get(ctx, "sample_force_back_2/cluster").Result()
		assert.Equal(t, e, redis.Nil)
		e = rdb.Del(ctx, "sample_force_back_2/cluster").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "")
	})

	t.Run("test Txn and PreCommit", func(t *testing.T) {
		data := NewSampleDataHubCluster()
		data.Uses("redis", sabi_redis.NewRedisClusterDataSrc(&redis.ClusterOptions{
			Addrs: []string{
				"127.0.0.1:7000",
				"127.0.0.1:7001",
				"127.0.0.1:7002",
			},
		}))

		ctx := context.Background()

		err := sabi.Txn(data, ctx, sampleLogicClusterWithPreCommit)
		assert.True(t, err.IsOk())

		rdb := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{
				"127.0.0.1:7000",
				"127.0.0.1:7001",
				"127.0.0.1:7002",
			},
		})

		s, e := rdb.Get(ctx, "sample_pre_commit/cluster").Result()
		assert.Nil(t, e)
		e = rdb.Del(ctx, "sample_pre_commit/cluster").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "Good Evening")
	})

	t.Run("test Txn and PostCommit", func(t *testing.T) {
		data := NewSampleDataHubCluster()
		data.Uses("redis", sabi_redis.NewRedisClusterDataSrc(&redis.ClusterOptions{
			Addrs: []string{
				"127.0.0.1:7000",
				"127.0.0.1:7001",
				"127.0.0.1:7002",
			},
		}))

		ctx := context.Background()

		err := sabi.Txn(data, ctx, sampleLogicClusterWithPostCommit)
		assert.True(t, err.IsOk())

		rdb := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{
				"127.0.0.1:7000",
				"127.0.0.1:7001",
				"127.0.0.1:7002",
			},
		})

		s, e := rdb.Get(ctx, "sample_post_commit/cluster").Result()
		assert.Nil(t, e)
		e = rdb.Del(ctx, "sample_post_commit/cluster").Err()
		assert.Nil(t, e)
		assert.Equal(t, s, "Good Night")
	})
}
