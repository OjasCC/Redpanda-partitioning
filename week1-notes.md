# Week 1 — Code Trace Notes

## Environment
- OS: Ubuntu 26.04 on WSL2 (Windows 11)
- Redpanda version: 26.1.7-1
- RPK version: 26.1.7-1

## Entry Point
- File: src/v/kafka/server/handlers/produce.cc
- Function: produce_handler::handle()
- Line: 639
- Signature: process_result_stages produce_handler::handle(request_context ctx, ss::smp_service_group ssg)
- What it does: Receives the incoming producer request with its
  context and SMP service group. The ss::smp_service_group in the
  signature is the first visible sign of the thread-per-core model.

## Write Path (Layer by Layer)

### Layer 1 — Kafka API Entry Point
- File: src/v/kafka/server/handlers/produce.cc
- Function: produce_handler::handle()
- Line: 639
- What it does: First function to receive the producer request.
  Routes it through validation and into partition handling.

### Layer 2 — Partition Routing
- File: src/v/kafka/server/handlers/produce.cc
- Function: partition_append()
- Line: 135
- Key call: partition.replicate() at Line 152
- Shard routing via: cluster/shard_table.h (included at Line 15)
- What it does: Assigns the message to the correct partition and
  calls replicate() which hands the entry off to Raft consensus.
  The shard_table inclusion confirms thread-per-core routing —
  each partition is mapped to a specific CPU shard.

### Layer 3 — Raft Consensus (Replication)
- File: src/v/raft/consensus.cc
- Replication request built at: Line 721 (append_entries_request)
- RPC call to followers at: Line 738 (.append_entries())
- Reply handler: process_append_entries_reply() at Line 578
- Success confirmation: successfull_append_entries_reply() at Line 593
- What it does: The partition leader builds an append_entries_request
  and sends it to all followers via RPC. It waits for a majority
  to confirm before marking the entry as committed. The producer
  is not acknowledged until this quorum is reached.

### Layer 4 — Physical Disk Write
- File: src/v/storage/disk_log_impl.cc
  Function: disk_log_impl::make_appender() at Line 2093
  Returns: disk_log_appender at Line 2105
- File: src/v/storage/disk_log_appender.cc
  Function: disk_log_appender::operator() at Line 81
  Key call: append_batch_to_segment() at Line 113
- File: src/v/storage/segment_appender.cc
  Function: segment_appender::append() at Line 111
  Direct I/O confirmed: disk_write_dma_alignment() at Line 65
- What it does: The batch flows through the log appender into the
  segment appender which performs a DMA (Direct Memory Access)
  write to disk, bypassing the OS page cache entirely.

## Complete Write Path Summary

produce_handler::handle()         [produce.cc:639]
        ↓
partition_append()                [produce.cc:135]
        ↓
partition.replicate()             [produce.cc:152]
        ↓
append_entries_request            [consensus.cc:721]
        ↓
.append_entries() RPC             [consensus.cc:738]
        ↓
disk_log_impl::make_appender()    [disk_log_impl.cc:2093]
        ↓
disk_log_appender::operator()     [disk_log_appender.cc:81]
        ↓
segment_appender::append()        [segment_appender.cc:111]
        ↓
DMA write to disk                 [segment_appender.cc:65]

## Key Classes Reference
| Class                  | File                                    | Role                        |
|------------------------|-----------------------------------------|-----------------------------|
| produce_handler        | kafka/server/handlers/produce.cc        | API entry point             |
| partition_append       | kafka/server/handlers/produce.cc        | Partition routing           |
| consensus              | raft/consensus.cc                       | Raft leader & replication   |
| disk_log_impl          | storage/disk_log_impl.cc               | Log management              |
| disk_log_appender      | storage/disk_log_appender.cc           | Batch to segment handoff    |
| segment_appender       | storage/segment_appender.cc            | Physical DMA disk write     |

## Three Key Questions — Answered

### 1. What function first receives the produce request?
produce_handler::handle() in
src/v/kafka/server/handlers/produce.cc at Line 639.
The ss::smp_service_group parameter confirms Seastar thread-per-core
routing is involved from the very first function.

### 2. How does Redpanda decide which partition a message goes to?
Via the shard_table (cluster/shard_table.h included at Line 15 of
produce.cc). Each partition is pre-assigned to a CPU shard. The
partition_append() function at Line 135 handles the routing.
For keyed messages, MurmurHash2 of the key determines the partition
number, which is then looked up in the shard_table to find the
correct CPU core to route the message to.

### 3. What function actually writes bytes to disk?
segment_appender::append() in
src/v/storage/segment_appender.cc at Line 111.
Direct I/O (DMA) is confirmed by disk_write_dma_alignment() at
Line 65, meaning Redpanda bypasses the OS page cache entirely
for predictable low-latency writes.
