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
	// RedisSentinelDataSrcNotSetupYet is an error reason which indicates that the data source is not setup yet.
	RedisSentinelDataSrcNotSetupYet struct{}
	// RedisSentinelDataSrcAlreadySetup is an error reason which indicates that the data source is already setup.
	RedisSentinelDataSrcAlreadySetup struct{}
	// RedisSentinelDataSrcFailToPing is an error reason which indicates that the data source failed to ping.
	RedisSentinelDataSrcFailToPing struct {
		Options *redis.Options
	}
)

// RedisSentinelDataConn is a data connection for Redis Sentinel.
type RedisSentinelDataConn struct {
	conn        *redis.Conn
	preCommits  [](func(*redis.Conn) errs.Err)
	postCommits [](func(*redis.Conn) errs.Err)
	forceBacks  [](func(*redis.Conn) errs.Err)
}

func newRedisSentinelDataConn(conn *redis.Conn) *RedisSentinelDataConn {
	return &RedisSentinelDataConn{
		conn:        conn,
		preCommits:  make([](func(*redis.Conn) errs.Err), 0),
		postCommits: make([](func(*redis.Conn) errs.Err), 0),
		forceBacks:  make([](func(*redis.Conn) errs.Err), 0),
	}
}

// GetConnection returns the underlying redis connection.
func (dc *RedisSentinelDataConn) GetConnection() *redis.Conn {
	return dc.conn
}

// AddPreCommit adds a function to be executed before commit.
func (dc *RedisSentinelDataConn) AddPreCommit(fn func(*redis.Conn) errs.Err) {
	dc.preCommits = append(dc.preCommits, fn)
}

// AddPostCommit adds a function to be executed after commit.
func (dc *RedisSentinelDataConn) AddPostCommit(fn func(*redis.Conn) errs.Err) {
	dc.postCommits = append(dc.postCommits, fn)
}

// AddForceBack adds a function to be executed on force back.
func (dc *RedisSentinelDataConn) AddForceBack(fn func(*redis.Conn) errs.Err) {
	dc.forceBacks = append(dc.forceBacks, fn)
}

// PreCommit executes pre-commit functions.
func (dc *RedisSentinelDataConn) PreCommit(ag *sabi.AsyncGroup) errs.Err {
	for _, f := range dc.preCommits {
		if err := f(dc.conn); err.IsNotOk() {
			return err
		}
	}
	return errs.Ok()
}

// Commit does nothing for this data connection.
func (dc *RedisSentinelDataConn) Commit(ag *sabi.AsyncGroup) errs.Err {
	return errs.Ok()
}

// PostCommit executes post-commit functions.
func (dc *RedisSentinelDataConn) PostCommit(ag *sabi.AsyncGroup) {
	for _, f := range dc.postCommits {
		_ = f(dc.conn)
	}
}

// ShouldForceBack returns true to indicate that this data connection should be forced back.
func (dc *RedisSentinelDataConn) ShouldForceBack() bool {
	return true
}

// Rollback does nothing for this data connection.
func (dc *RedisSentinelDataConn) Rollback(ag *sabi.AsyncGroup) {}

// ForceBack executes force-back functions in reverse order.
func (dc *RedisSentinelDataConn) ForceBack(ag *sabi.AsyncGroup) {
	for _, f := range slices.Backward(dc.forceBacks) {
		_ = f(dc.conn)
	}
}

// Close does nothing for this data connection.
func (dc *RedisSentinelDataConn) Close() {}

// RedisSentinelDataSrc is a data source for Redis Sentinel.
type RedisSentinelDataSrc struct {
	options *redis.FailoverOptions
	client  *redis.Client
}

// NewRedisSentinelDataSrc creates a new RedisSentinelDataSrc.
func NewRedisSentinelDataSrc(opt *redis.FailoverOptions) *RedisSentinelDataSrc {
	return &RedisSentinelDataSrc{
		options: opt,
	}
}

// Setup initializes the Redis client and pings the server.
func (ds *RedisSentinelDataSrc) Setup(ag *sabi.AsyncGroup) errs.Err {
	if ds.options == nil {
		return errs.New(RedisSentinelDataSrcAlreadySetup{})
	}

	ds.client = redis.NewFailoverClient(ds.options)
	ds.options = nil
	_, e := ds.client.Ping(context.Background()).Result()
	if e != nil {
		return errs.New(RedisSentinelDataSrcFailToPing{Options: ds.client.Options()}, e)
	}
	return errs.Ok()
}

// Close closes the Redis client.
func (ds *RedisSentinelDataSrc) Close() {
	if ds.client != nil {
		c := ds.client
		ds.client = nil
		c.Close()
	}
}

// CreateDataConn creates a new RedisSentinelDataConn.
func (ds *RedisSentinelDataSrc) CreateDataConn() (sabi.DataConn, errs.Err) {
	if ds.client == nil {
		return nil, errs.New(RedisSentinelDataSrcNotSetupYet{})
	}
	return newRedisSentinelDataConn(ds.client.Conn()), errs.Ok()
}
