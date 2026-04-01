// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi_redis

import (
	"context"
	"slices"

	"github.com/redis/go-redis/v9"
	"github.com/sttk/errs"
	"github.com/sttk/sabi"
)

type /* error reasons */ (
	// RedisClusterDataSrcNotSetupYet is an error reason which indicates that the data source is not
	// setup yet.
	RedisClusterDataSrcNotSetupYet struct{}

	// RedisClusterDataSrcAlreadySetup is an error reason which indicates that the data source is
	// already setup.
	RedisClusterDataSrcAlreadySetup struct{}

	// RedisClusterDataSrcFailToPing is an error reason which indicates that the data source failed
	// to ping.
	RedisClusterDataSrcFailToPing struct {
		Options *redis.ClusterOptions
	}
)

// RedisClusterDataConn is a data connection for Redis Cluster.
type RedisClusterDataConn struct {
	conn        *redis.ClusterClient
	preCommits  [](func(*redis.ClusterClient) errs.Err)
	postCommits [](func(*redis.ClusterClient) errs.Err)
	forceBacks  [](func(*redis.ClusterClient) errs.Err)
}

func newRedisClusterDataConn(conn *redis.ClusterClient) *RedisClusterDataConn {
	return &RedisClusterDataConn{
		conn:        conn,
		preCommits:  make([](func(*redis.ClusterClient) errs.Err), 0),
		postCommits: make([](func(*redis.ClusterClient) errs.Err), 0),
		forceBacks:  make([](func(*redis.ClusterClient) errs.Err), 0),
	}
}

// GetConnection returns the underlying redis cluster client.
func (dc *RedisClusterDataConn) GetConnection() *redis.ClusterClient {
	return dc.conn
}

// AddPreCommit adds a function to be executed before commit.
func (dc *RedisClusterDataConn) AddPreCommit(fn func(*redis.ClusterClient) errs.Err) {
	dc.preCommits = append(dc.preCommits, fn)
}

// AddPostCommit adds a function to be executed after commit.
func (dc *RedisClusterDataConn) AddPostCommit(fn func(*redis.ClusterClient) errs.Err) {
	dc.postCommits = append(dc.postCommits, fn)
}

// AddForceBack adds a function to be executed on force back.
func (dc *RedisClusterDataConn) AddForceBack(fn func(*redis.ClusterClient) errs.Err) {
	dc.forceBacks = append(dc.forceBacks, fn)
}

// PreCommit executes pre-commit functions.
func (dc *RedisClusterDataConn) PreCommit(ag *sabi.AsyncGroup) errs.Err {
	for _, f := range dc.preCommits {
		if err := f(dc.conn); err.IsNotOk() {
			return err
		}
	}
	return errs.Ok()
}

// Commit does nothing for this data connection.
func (dc *RedisClusterDataConn) Commit(ag *sabi.AsyncGroup) errs.Err {
	return errs.Ok()
}

// PostCommit executes post-commit functions.
func (dc *RedisClusterDataConn) PostCommit(ag *sabi.AsyncGroup) {
	for _, f := range dc.postCommits {
		_ = f(dc.conn)
	}
}

// ShouldForceBack returns true to indicate that this data connection should be forced back.
func (dc *RedisClusterDataConn) ShouldForceBack() bool {
	return true
}

// Rollback does nothing for this data connection.
func (dc *RedisClusterDataConn) Rollback(ag *sabi.AsyncGroup) {}

// ForceBack executes force-back functions in reverse order.
func (dc *RedisClusterDataConn) ForceBack(ag *sabi.AsyncGroup) {
	for _, f := range slices.Backward(dc.forceBacks) {
		_ = f(dc.conn)
	}
}

// Close does nothing for this data connection.
func (dc *RedisClusterDataConn) Close() {}

// RedisClusterDataSrc is a data source for Redis Cluster.
type RedisClusterDataSrc struct {
	options *redis.ClusterOptions
	client  *redis.ClusterClient
}

// NewRedisClusterDataSrc creates a new RedisClusterDataSrc.
func NewRedisClusterDataSrc(opt *redis.ClusterOptions) *RedisClusterDataSrc {
	return &RedisClusterDataSrc{
		options: opt,
	}
}

// Setup initializes the Redis cluster client and pings the cluster.
func (ds *RedisClusterDataSrc) Setup(ag *sabi.AsyncGroup) errs.Err {
	if ds.options == nil {
		return errs.New(RedisClusterDataSrcAlreadySetup{})
	}

	ds.client = redis.NewClusterClient(ds.options)
	ds.options = nil
	_, e := ds.client.Ping(context.Background()).Result()
	if e != nil {
		return errs.New(RedisClusterDataSrcFailToPing{Options: ds.client.Options()}, e)
	}
	return errs.Ok()
}

// Close closes the Redis cluster client.
func (ds *RedisClusterDataSrc) Close() {
	if ds.client != nil {
		c := ds.client
		ds.client = nil
		c.Close()
	}
}

// CreateDataConn creates a new RedisClusterDataConn.
func (ds *RedisClusterDataSrc) CreateDataConn() (sabi.DataConn, errs.Err) {
	if ds.client == nil {
		return nil, errs.New(RedisClusterDataSrcNotSetupYet{})
	}
	return newRedisClusterDataConn(ds.client), errs.Ok()
}
