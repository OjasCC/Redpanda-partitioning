# Introduction

## What is Redpanda?
Redpanda is a Kafka-API-compatible streaming platform written in C++
on top of the Seastar framework. It requires no ZooKeeper and has no JVM.
It is fully compatible with Kafka clients and tools.

## What This Project Studies
This project traces Redpanda's partitioning mechanism at the source-code
level. The focus is on:
- How messages are assigned to partitions (murmur2 key hashing)
- How partitions are replicated across nodes via Raft consensus
- How partitions survive node failure via Raft leader election
- How partition count affects throughput via thread-per-core scaling

## Core Files This Project is Anchored To
| File                                          | Role                        |
|-----------------------------------------------|-----------------------------|
| src/v/kafka/server/handlers/produce.cc        | Producer API entry point    |
| src/v/cluster/partition.cc                    | Partition management        |
| src/v/cluster/shard_table.h                   | Thread-per-core routing     |
| src/v/raft/consensus.cc                       | Raft replication & election |
| src/v/storage/disk_log_impl.cc                | Log management              |
| src/v/storage/disk_log_appender.cc            | Batch to segment handoff    |
| src/v/storage/segment_appender.cc             | Physical DMA disk write     |
| src/v/hashing/murmur.h                        | Partition key hashing       |

## Why Redpanda's Partitioning is Interesting
- Thread-per-core: each partition owned by exactly one CPU core
- Built-in Raft: no ZooKeeper needed for partition leader election
- DMA writes: no page cache interference for partition storage
- murmur2 hashing: Kafka-compatible partition key assignment
- C++ with no GC: predictable partition write latency

## Environment
- OS: Ubuntu 26.04 on WSL2 (Windows 11)
- Redpanda version: 26.1.7-1
- RPK version: 26.1.7-1
- CPU cores available: (fill from `nproc`)
