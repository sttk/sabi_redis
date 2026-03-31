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
	// NotSetupYet is an error reason which indicates that the data source is not setup yet.
	NotSetupYet struct{}
	// AlreadySetup is an error reason which indicates that the data source is already setup.
	AlreadySetup struct{}
	// FailToPing is an error reason which indicates that the data source failed to ping.
	FailToPing struct {
		Options *redis.Options
	}
)

// RedisDataConn is a data connection for Redis.
type RedisDataConn struct {
	conn        *redis.Conn
	preCommits  [](func(*redis.Conn) errs.Err)
	postCommits [](func(*redis.Conn) errs.Err)
	forceBacks  [](func(*redis.Conn) errs.Err)
}

func newRedisDataConn(conn *redis.Conn) *RedisDataConn {
	return &RedisDataConn{
		conn:        conn,
		preCommits:  make([](func(*redis.Conn) errs.Err), 0),
		postCommits: make([](func(*redis.Conn) errs.Err), 0),
		forceBacks:  make([](func(*redis.Conn) errs.Err), 0),
	}
}

// GetConnection returns the underlying redis connection.
func (dc *RedisDataConn) GetConnection() *redis.Conn {
	return dc.conn
}

// AddPreCommit adds a function to be executed before commit.
func (dc *RedisDataConn) AddPreCommit(fn func(*redis.Conn) errs.Err) {
	dc.preCommits = append(dc.preCommits, fn)
}

// AddPostCommit adds a function to be executed after commit.
func (dc *RedisDataConn) AddPostCommit(fn func(*redis.Conn) errs.Err) {
	dc.postCommits = append(dc.postCommits, fn)
}

// AddForceBack adds a function to be executed on force back.
func (dc *RedisDataConn) AddForceBack(fn func(*redis.Conn) errs.Err) {
	dc.forceBacks = append(dc.forceBacks, fn)
}

// PreCommit executes pre-commit functions.
func (dc *RedisDataConn) PreCommit(ag *sabi.AsyncGroup) errs.Err {
	for _, f := range dc.preCommits {
		if err := f(dc.conn); err.IsNotOk() {
			return err
		}
	}
	return errs.Ok()
}

// Commit does nothing for this data connection.
func (dc *RedisDataConn) Commit(ag *sabi.AsyncGroup) errs.Err {
	return errs.Ok()
}

// PostCommit executes post-commit functions.
func (dc *RedisDataConn) PostCommit(ag *sabi.AsyncGroup) {
	for _, f := range dc.postCommits {
		_ = f(dc.conn)
	}
}

// ShouldForceBack returns true to indicate that this data connection should be forced back.
func (dc *RedisDataConn) ShouldForceBack() bool {
	return true
}

// Rollback does nothing for this data connection.
func (dc *RedisDataConn) Rollback(ag *sabi.AsyncGroup) {}

// ForceBack executes force-back functions in reverse order.
func (dc *RedisDataConn) ForceBack(ag *sabi.AsyncGroup) {
	for _, f := range slices.Backward(dc.forceBacks) {
		_ = f(dc.conn)
	}
}

// Close does nothing for this data connection.
func (dc *RedisDataConn) Close() {}

// RedisDataSrc is a data source for Redis.
type RedisDataSrc struct {
	options *redis.Options
	client  *redis.Client
}

// NewRedisDataSrc creates a new RedisDataSrc.
func NewRedisDataSrc(opt *redis.Options) *RedisDataSrc {
	return &RedisDataSrc{
		options: opt,
	}
}

// Setup initializes the Redis client and pings the server.
func (ds *RedisDataSrc) Setup(ag *sabi.AsyncGroup) errs.Err {
	if ds.options == nil {
		return errs.New(AlreadySetup{})
	}

	ds.client = redis.NewClient(ds.options)
	ds.options = nil
	_, e := ds.client.Ping(context.Background()).Result()
	if e != nil {
		return errs.New(FailToPing{Options: ds.client.Options()}, e)
	}
	return errs.Ok()
}

// Close closes the Redis client.
func (ds *RedisDataSrc) Close() {
	if ds.client != nil {
		c := ds.client
		ds.client = nil
		c.Close()
	}
}

// CreateDataConn creates a new RedisDataConn.
func (ds *RedisDataSrc) CreateDataConn() (sabi.DataConn, errs.Err) {
	return newRedisDataConn(ds.client.Conn()), errs.Ok()
}
