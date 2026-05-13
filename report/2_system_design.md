# System Design Decisions & Concept Mapping

## Design Decision 1: Thread-per-Core Architecture (Seastar)
- **What:** Each CPU core owns a fixed set of partitions with no shared
  state between cores. Every incoming message is routed to the correct
  shard before any processing begins.
- **Where in code:**
  - Shard table included: src/v/kafka/server/handlers/produce.cc Line 15
  - Partition to shard lookup: produce.cc Line 259
    `auto shard = octx.rctx.shards().shard_for(req.ntp)`
  - Message dispatched to shard: produce.cc Line 302
  - Shard table definition: src/v/cluster/shard_table.h
    `shard_for(const T& ntp)` at Line 47
    Each partition stores its shard_id at Line 31
- **Problem solved:** Eliminates mutex contention and context switching.
  Each core works completely independently with no locks needed.
- **Trade-off:** Cross-shard operations require explicit message passing,
  adding latency. No automatic load rebalancing between shards.
- **Course concept:** Partitioning — data divided and isolated per core

---

## Design Decision 2: Raft Replication Before Acknowledgment
- **What:** The partition leader replicates a write to a quorum of
  followers via Raft before acknowledging the producer. The producer
  is blocked until replication is confirmed.
- **Where in code:**
  - Raft options: produce.cc Lines 71-79
    acks_to_replicate_options() maps Kafka acks to Raft levels:
    acks=-1 → quorum_ack, acks=0 → no_ack, acks=1 → leader_ack
  - Raft handoff: produce.cc Line 152
    partition.replicate() called with consistency options
  - Producer waits: produce.cc Line 156
    .replicate_finished.then_wrapped()
  - Raft call in partition: src/v/cluster/partition.cc Line 348
    co_await _raft->replicate() — waits for Raft to complete
  - Final return: partition.cc Line 383
    co_return co_await replicate_finished
  - Replication request: src/v/raft/consensus.cc Line 721
  - RPC to followers: consensus.cc Line 738
  - Reply handler: consensus.cc Line 578
- **Problem solved:** Guarantees no acknowledged write is ever lost
  even if the leader crashes immediately after acking.
- **Trade-off:** Every produce call pays a network round-trip latency
  penalty waiting for follower acknowledgment.
- **Course concept:** Replication and fault tolerance

---

## Design Decision 3: Append-Only Log with Direct I/O (DMA)
- **What:** Messages are only ever appended to segment files, never
  overwritten. Redpanda uses DMA writes to bypass the OS page cache.
- **Where in code:**
  - DMA alignment check: src/v/storage/segment_appender.cc Line 65
    disk_write_dma_alignment()
  - Actual DMA write: segment_appender.cc Line 648
    .dma_write() — bypasses OS page cache entirely
  - Bytes tracked: segment_appender.cc Line 654
    bytes_written += dma_size
  - Final append function: segment_appender.cc Line 111
    segment_appender::append()
  - Flush safety enforced: segment_appender.cc Lines 76-77
    segment cannot close without flushing first
  - Log appender created: src/v/storage/disk_log_impl.cc Line 2093
    disk_log_impl::make_appender()
- **Problem solved:** Sequential appends are far faster than random
  writes. DMA gives predictable latency with no page cache eviction.
- **Trade-off:** Compaction needed to reclaim old segment space.
  DMA requires memory-aligned buffers.
- **Course concept:** Storage and streaming ingestion

---

## Partition Key Hashing
- **File:** src/v/hashing/murmur.h
- **Function:** murmur2() with seed 0x9747b28c
- **Comment in code:** "Default Seed is the Kafka partition hashing seed"
- **How it works:** murmur2(message_key) % partition_count = partition_number
- **Kafka compatibility:** Same key produces same partition in both
  Kafka and Redpanda — fully compatible hashing
- **Impact on experiments:** Fixed key always hashes to same partition
  → partition skew (Experiment 3)

---

## Raft Leader Election
- **File:** src/v/raft/consensus.cc
- **Vote states:** follower → candidate → leader (Lines 62-75)
- **Election trigger:** _vote_timeout → dispatch_vote() (Lines 162-164)
- **Heartbeat tracking:** _heartbeat_disconnect_failures (Lines 142-143)
- **Leader step down:** do_step_down("heartbeats_majority") (Line 244)
- **Leader lost:** _leader_id cleared → trigger_leadership_notification()
  (Lines 245-247)
- **Non-leader rejection:** not_leader error (Line 689)
- **Impact on experiments:** Observed in Experiment 2 — failover window
  is the time between leader loss and new leader notification firing

---

## Course Concept Mapping

| Course Concept      | Redpanda Implementation                | Code Reference                              |
|---------------------|----------------------------------------|---------------------------------------------|
| Partitioning        | Topic split into N shards, one per core| produce.cc:259 → shard_table.h:47          |
| Replication         | Raft consensus per partition           | consensus.cc:721 → append_entries RPC       |
| Fault Tolerance     | Raft leader election on crash          | consensus.cc:162 → dispatch_vote()          |
| Storage             | Append-only segments, DMA writes       | segment_appender.cc:648 → dma_write()       |
| Streaming/Ingestion | Partition leader handles produce path  | produce.cc:639 → produce_handler::handle()  |
