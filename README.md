# [sabi_redis][repo-url] [![Go Reference][pkg-dev-img]][pkg-dev-url] [![CI Status][ci-img]][ci-url] [![MIT License][mit-img]][mit-url]

`sabi_redis` is a library that provides data access for Redis within the [sabi][sabi-repo] framework.
It allows for a streamlined and consistent way to access various Redis configurations while adhering to the architectural patterns of the sabi framework.

## Key Features

- **Multi-Configuration Support**: Specialized implementations for Standalone Redis, Redis Sentinel, and Redis Cluster.
- **Consistency Management**: Since Redis lacks native rollbacks, this library provides mechanisms to manage data integrity:
  - **Pre-commit**: Logic executed before the main commit phase.
  - **Post-commit**: Logic executed after a successful commit.
  - **Force-back**: A mechanism to register "revert" logic that executes if a transaction fails.

## Installation

```bash
go get github.com/sttk/sabi_redis
```

## Usage

### 1. Define a Data Access

Implement a data access struct that retrieves the Redis connection from the sabi context.

```go
type RedisSampleDataAcc struct {
    sabi.DataAcc
}

func (da *RedisSampleDataAcc) SetValue(key, val string) errs.Err {
    ctx := da.Context()
    dc, err := sabi.GetDataConn[*sabi_redis.RedisDataConn](da, "redis")
    if err.IsNotOk() {
        return err
    }
    
    redisConn := dc.GetConnection()
    e := redisConn.Set(ctx, key, val, 0).Err()
    if e != nil {
        return errs.New(FailToSetValue{}, e)
    }

    // Register a force-back logic to delete the key if the transaction fails
    dc.AddForceBack(func(conn *redis.Conn) errs.Err {
        return errs.NewFromErr(conn.Del(ctx, key).Err())
    })

    return errs.Ok()
}
```

### 2. Register and Setup the Data Source

Register the appropriate Redis data source.

```go
// Standalone
ds := sabi_redis.NewRedisDataSrc(&redis.Options{Addr: "localhost:6379"})

// Or Sentinel
// ds := sabi_redis.NewRedisSentinelDataSrc(&redis.FailoverOptions{...})

// Or Cluster
// ds := sabi_redis.NewRedisClusterDataSrc(&redis.ClusterOptions{...})
```

### 3. Define logic and its data interface

Define the business logic and the interface it requires.

```go
type SampleData interface {
    SetValue(key, val string) errs.Err
}

func sampleLogic(data SampleData) errs.Err {
    return data.SetValue("mykey", "myvalue")
}
```

### 4. Integrate DataAcc into DataHub

Create a `DataHub` that integrates the `DataAcc`.

```go
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

var _ SampleData = (*SampleDataHub)(nil) // Check if SampleDataHub implements SampleData at compile time
```

### 5. Run within a transaction

Execute your business logic using the `DataHub`.

```go
ctx := context.Background()
hub := NewSampleDataHub()
hub.Uses("redis", ds)

err := sabi.Run(hub, ctx, sampleLogic)
```

## Supporting Go versions

This library supports Go 1.23 or later.

### Actual test results for each Go version:

```sh
% go-fav 1.23.12 1.24.13 1.25.8 1.26.1
go version go1.23.12 darwin/amd64
ok  	github.com/sttk/sabi_redis	9.414s	coverage: 77.9% of statements

go version go1.24.13 darwin/amd64
ok  	github.com/sttk/sabi_redis	9.253s	coverage: 77.9% of statements

go version go1.25.8 darwin/amd64
ok  	github.com/sttk/sabi_redis	9.356s	coverage: 77.9% of statements

go version go1.26.1 darwin/amd64
ok  	github.com/sttk/sabi_redis	9.295s	coverage: 77.9% of statements
```

## License

Copyright (C) 2026 Takayuki Sato

This program is free software under MIT License.<br>
See the file LICENSE in this distribution for more details.

[repo-url]: https://github.com/sttk/sabi_redis
[sabi-repo]: https://github.com/sttk/sabi
[pkg-dev-img]: https://pkg.go.dev/badge/github.com/sttk/sabi_redis.svg
[pkg-dev-url]: https://pkg.go.dev/github.com/sttk/sabi_redis
[ci-img]: https://github.com/sttk/sabi_redis/actions/workflows/go.yml/badge.svg?branch=main
[ci-url]: https://github.com/sttk/sabi_redis/actions?query=branch%3Amain
[mit-img]: https://img.shields.io/badge/license-MIT-green.svg
[mit-url]: https://opensource.org/licenses/MIT
