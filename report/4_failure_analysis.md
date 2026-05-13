# Failure Analysis

## Q1: What happens when data size increases significantly across partitions?
- Each partition's segment files grow until retention limit is hit
- Log compaction kicks in to reclaim disk space
- Large partitions increase per-entry Raft replication overhead
- Storage I/O becomes the bottleneck before CPU does
- Code reference: src/v/storage/disk_log_impl.cc — segment rolling
                  src/v/storage/segment_appender.cc Lines 76-77
                  segment cannot close without flushing first
- DMA write size increases: segment_appender.cc Line 654
  bytes_written += dma_size tracked per operation

## Q2: What happens under partition skew?
- One partition receives majority of traffic (proven in Experiment 3)
- Fixed key sent 100% to Partition 1 — other two partitions idle
- The single Seastar shard owning that partition becomes saturated
- 9.4% throughput loss observed experimentally
- At production scale (millions msgs/sec) this becomes critical
- Code reference: src/v/hashing/murmur.h — murmur2() seed 0x9747b28c
                  src/v/kafka/server/handlers/produce.cc Line 259
                  shard_for() — same key always same shard
- Fix: use null keys for round-robin assignment, redesign key space

## Q3: What happens if a partition leader crashes?
- Raft detects missing heartbeat via _vote_timeout (consensus.cc:162)
- dispatch_vote() triggered — election begins (consensus.cc:164)
- _leader_id cleared, trigger_leadership_notification() called (Line 247)
- New leader elected among remaining replicas
- Proven in Experiment 2 — 7230.2ms spike at message 33
- Immediate recovery — message 34 back to 2.3ms latency
- Zero data loss — Raft quorum guarantee holds
- Code reference: src/v/raft/consensus.cc Lines 162-164, 245-247
                  src/v/cluster/partition.cc Line 383

## Q4: What underlying assumptions does Redpanda's partitioning rely on?
- Message keys are well-distributed across the key space
  (murmur2 only helps if keys are varied — proven in Experiment 3)
- Partition count is tuned to match client and broker capabilities
  (proven in Experiment 1 — peak at 3 not 8 despite 8 cores)
- Network latency between Raft replicas is low and stable
  (high latency would extend the 7.2s election window further)
- Disk supports fast sequential DMA writes
  (segment_appender.cc Line 648 — dma_write() assumption)
- Producers are configured with appropriate acks and retry settings
  (acks=all + retries=10 gave zero errors in Experiment 2)
