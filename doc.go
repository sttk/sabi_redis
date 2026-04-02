// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

/*
Package sabi_redis is a library that provides data access for Redis within the sabi framework.

This library allows for a streamlined and consistent way to access various Redis configurations
while adhering to the architectural patterns of the sabi framework.

# Key Features

  - Multi-Configuration Support: Provides specialized implementations for Standalone Redis
    servers, Redis Sentinel for high availability, and Redis Cluster for horizontal scaling.
  - Consistency Management: Since Redis lacks native rollbacks, this library provides
    mechanisms to manage data integrity during failures:
    - Pre-commit: Logic executed before the main commit phase.
    - Post-commit: Logic executed after a successful commit.
    - Force-back: A mechanism to register "revert" logic that executes if a transaction fails.

# Integration with the sabi framework

This library integrates with sabi by implementing its core data access abstractions:

  - DataSrc & DataConn: It provides RedisDataSrc (and variants like RedisSentinelDataSrc and
    RedisClusterDataSrc) to handle connection pooling and configuration, and RedisDataConn
    (and its variants) to provide the actual connection to business logic.
  - Registration & Setup: Data sources are registered using the framework's "Uses" function
    and initialized during the setup phase.
  - Transaction Management: Redis operations are typically performed within a sabi transaction
    (Dab.Txn).
  - Error Handling: It utilizes the framework's transaction lifecycle to trigger "ForceBack"
    operations, allowing developers to attempt to undo Redis changes if a transaction fails.
*/
package sabi_redis
