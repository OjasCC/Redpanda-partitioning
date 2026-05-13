# Redpanda — Partitioning Deep Dive

**Course:** Big Data Engineering  

**Group Member 1:** Harsh Jethwani (202518055) 

**Group Member 2:** Ojas Gupta (202518057)  
             
**System:** Redpanda — Partitioning Subsystem  
**Source:** [github.com/redpanda-data/redpanda](https://github.com/redpanda-data/redpanda) (cloned locally)  
**Environment:** Ubuntu 26.04 LTS on WSL2 (Windows 11) | Redpanda v26.1.7-1 | Python 3.14

---

## 1. What Problem Does This System Solve?

Redpanda is a Kafka-API-compatible streaming platform written in C++ on top of the Seastar framework. Unlike Kafka, it has no ZooKeeper dependency and no JVM runtime. Users produce messages to named topics — Redpanda distributes those messages across partitions, replicates them via Raft consensus, and writes them to disk using Direct Memory Access (DMA) writes.

The **Partitioning subsystem** is the machinery that takes an incoming producer request and decides:
- Which partition the message belongs to (based on key hashing)
- Which CPU shard owns that partition (thread-per-core routing)
- How to replicate the write across nodes before acknowledging the producer
- How to physically write the data to disk with predictable latency

This sounds simple, but it is a hard engineering problem because:
- Partition assignment must be **deterministic and Kafka-compatible**
- Replication must happen **before acknowledgment** to guarantee durability
- Storage must be **append-only with DMA** for predictable latency
- CPU work must be **isolated per core** to avoid lock contention

### Files Analyzed

| File | Location | Role |
|---|---|---|
| `produce.cc` | `src/v/kafka/server/handlers/produce.cc` | API entry point — receives producer request |
| `partition.cc` | `src/v/cluster/partition.cc` | Partition management — calls Raft |
| `shard_table.h` | `src/v/cluster/shard_table.h` | Thread-per-core routing table |
| `consensus.cc` | `src/v/raft/consensus.cc` | Raft replication and leader election |
| `disk_log_impl.cc` | `src/v/storage/disk_log_impl.cc` | Log management — creates appender |
| `disk_log_appender.cc` | `src/v/storage/disk_log_appender.cc` | Batch to segment handoff |
| `segment_appender.cc` | `src/v/storage/segment_appender.cc` | Physical DMA disk write |
| `murmur.h` | `src/v/hashing/murmur.h` | MurmurHash2 — partition key hashing |

---

## 2. Execution Path: Producer Request → Disk

### Step 1: Entry Point — `produce.cc:639`

The Kafka-compatible API receives the producer request here:

```cpp
// produce.cc, line 639
process_result_stages
produce_handler::handle(request_context ctx, ss::smp_service_group ssg)
```

The `ss::smp_service_group` parameter is the first visible sign of the thread-per-core model — `smp` stands for Symmetric Multi-Processing, Seastar's CPU shard management system.

### Step 2: Shard Routing — `produce.cc:259`

Before any processing, the message is routed to the correct CPU shard:

```cpp
// produce.cc, line 259
auto shard = octx.rctx.shards().shard_for(req.ntp);
if (shard) {
    // dispatch to correct CPU core
    *shard, ...  // line 302
}
```

`shard_for(ntp)` is defined in `shard_table.h:47`. Each partition is pre-assigned to exactly one CPU core. No shared state. No locks.

### Step 3: Partition Append — `produce.cc:135`

```cpp
// produce.cc, line 135
partition_produce_stages partition_append(
  model::partition_id id,
  partition_proxy partition, ...

// produce.cc, line 152
auto stages = partition.replicate(
    bid, std::move(*batch), acks_to_replicate_options(acks, timeout_ms));
```

The `acks` setting is converted to a Raft consistency level:

```cpp
// produce.cc, lines 71-79
raft::replicate_options acks_to_replicate_options(...) {
    case -1: return {raft::consistency_level::quorum_ack, timeout};   // wait for majority
    case  0: return {raft::consistency_level::no_ack, timeout};       // fire and forget
    case  1: return {raft::consistency_level::leader_ack, timeout};   // leader only
}
```

### Step 4: Raft Replication — `partition.cc:348`

```cpp
// partition.cc, line 339
ss::future<result<kafka_result>> partition::replicate(
  chunked_vector<model::record_batch> batches, raft::replicate_options opts) {

    auto maybe_units = co_await hold_writes_enabled();  // line 343 — write guard
    auto res = co_await _raft->replicate(std::move(batches), opts);  // line 348
    ...
    co_return co_await std::move(orig_stages.replicate_finished);  // line 383
```

The `co_await` on line 348 means the partition layer **waits for Raft to complete** before returning. The producer is not acknowledged until `replicate_finished` resolves at line 383.

### Step 5: Raft Consensus — `consensus.cc:721`

```cpp
// consensus.cc, line 721
append_entries_request req(...)

// consensus.cc, line 738
.append_entries(req, rpc::client_opts(_replicate_append_timeout))
```

The leader builds an `append_entries_request` and sends it to all followers via RPC. Replies are processed at:

```cpp
// consensus.cc, line 578
void consensus::process_append_entries_reply(...)

// consensus.cc, line 593
void consensus::successfull_append_entries_reply(
  follower_index_metadata& idx, const append_entries_reply& reply)
```

### Step 6: Log Appender — `disk_log_impl.cc:2093`

```cpp
// disk_log_impl.cc, line 2093
log_appender disk_log_impl::make_appender(log_append_config cfg) {
    ...
    return log_appender(
      std::make_unique<disk_log_appender>(*this, cfg, now, next_offset));  // line 2105
}
```

### Step 7: Segment Handoff — `disk_log_appender.cc:81`

```cpp
// disk_log_appender.cc, line 81
disk_log_appender::operator()(model::record_batch& batch) {
    ...
    auto stop = co_await append_batch_to_segment(batch);  // line 113
}
```

### Step 8: DMA Write to Disk — `segment_appender.cc:648`

```cpp
// segment_appender.cc, line 65
const auto alignment = _out.disk_write_dma_alignment();  // Direct I/O alignment

// segment_appender.cc, line 648
.dma_write(                     // ← ACTUAL DISK WRITE — bypasses OS page cache
    w->chunk_begin,
    chunk_data,
    dma_size)
.then([this, w, dma_size](size_t got) {
    _opts.shared_stats->bytes_written += dma_size;  // line 654
```

### Complete Write Path Summary

```
produce_handler::handle()         [produce.cc:639]
        ↓
shard_for(req.ntp)                [produce.cc:259]     ← thread-per-core routing
        ↓
partition_append()                [produce.cc:135]
        ↓
partition.replicate()             [produce.cc:152]
        ↓
_raft->replicate()                [partition.cc:348]   ← co_await — blocks until done
        ↓
append_entries_request            [consensus.cc:721]
        ↓
.append_entries() RPC             [consensus.cc:738]   ← sent to all followers
        ↓
process_append_entries_reply()    [consensus.cc:578]   ← quorum confirmed
        ↓
disk_log_impl::make_appender()    [disk_log_impl.cc:2093]
        ↓
disk_log_appender::operator()     [disk_log_appender.cc:81]
        ↓
segment_appender::append()        [segment_appender.cc:111]
        ↓
.dma_write()                      [segment_appender.cc:648]  ← bytes hit disk
```

---

## 3. Design Decisions & Trade-offs

### Decision 1: Thread-per-Core Architecture (Seastar)

**Code:** `produce.cc:259`, `shard_table.h:47`, `cluster/partition_manager.cc`

**Problem:** Traditional multi-threaded systems use shared thread pools — any thread can handle any partition. This requires locks on every shared data structure, causing contention at high throughput.

**Solution:** Each partition is permanently assigned to exactly one CPU shard. `shard_for(ntp)` at `shard_table.h:47` looks up this assignment:

```cpp
// shard_table.h, line 31
ss::shard_id shard;  // each partition entry stores its CPU core

// shard_table.h, line 47
std::optional<ss::shard_id> shard_for(const T& ntp)  // lookup by partition
```

| Pros | Cons |
|---|---|
| Zero lock contention — each core owns its data | No automatic load rebalancing across shards |
| Predictable cache locality per core | Cross-shard operations require message passing |
| Linear scalability with core count | Partition skew saturates one core while others idle |

**Experiment 1 proves this:** Throughput peaked at 3 partitions — adding more partitions beyond the optimal point adds management overhead without parallelism gain.

---

### Decision 2: Raft Replication Before Acknowledgment

**Code:** `produce.cc:71-79`, `partition.cc:348`, `consensus.cc:721`

**Problem:** If a broker crashes after writing locally but before replicating, acknowledged data is lost.

**Solution:** The produce handler converts the Kafka `acks` setting into a Raft consistency level and `co_await`s replication before responding:

```cpp
// produce.cc, line 156
.produced = stages.replicate_finished.then_wrapped(...)
```

The producer is blocked at `partition.cc:383`:

```cpp
co_return co_await std::move(orig_stages.replicate_finished);
```

Until `consensus.cc:593` confirms a quorum of followers have acknowledged:

```cpp
void consensus::successfull_append_entries_reply(
  follower_index_metadata& idx, const append_entries_reply& reply)
```

| Pros | Cons |
|---|---|
| Zero data loss for acknowledged writes | Every produce call waits for a network round-trip |
| Configurable via acks (-1, 0, 1) | Leader crash during replication adds latency spike |
| Raft handles leader election automatically | Replication adds ~2-4ms to normal latency |

**Experiment 2 proves this:** With `acks=all`, zero data loss observed even when the leader was killed mid-produce. The 7230.2ms spike at message 33 is the Raft election window.

---

### Decision 3: Append-Only Log with Direct I/O (DMA)

**Code:** `segment_appender.cc:65`, `segment_appender.cc:648`, `disk_log_impl.cc:2093`

**Problem:** Random writes to disk are orders of magnitude slower than sequential writes. OS page cache introduces unpredictable eviction latency.

**Solution:** Messages are only ever appended to segment files — never overwritten. All writes use DMA, bypassing the OS page cache:

```cpp
// segment_appender.cc, line 65
const auto alignment = _out.disk_write_dma_alignment();  // enforce alignment

// segment_appender.cc, line 648
.dma_write(chunk_begin, chunk_data, dma_size)  // direct to disk, no page cache
```

Flush safety is enforced:

```cpp
// segment_appender.cc, lines 76-77
"Must flush & close before deleting {}"  // segment cannot close unflushed
```

| Pros | Cons |
|---|---|
| Sequential writes — maximum disk throughput | Compaction needed to reclaim old segment space |
| DMA gives predictable tail latency | DMA requires memory-aligned buffers |
| No page cache eviction surprises | Append-only means reads must scan segments |

---

### Decision 4: MurmurHash2 for Partition Assignment

**Code:** `src/v/hashing/murmur.h`

**Problem:** Messages must be routed to partitions deterministically — same key must always go to same partition, and distribution must be uniform.

**Solution:** MurmurHash2 with the Kafka-compatible seed `0x9747b28c`:

```cpp
// hashing/murmur.h
uint32_t murmur2(
  const void* key,
  std::size_t len,
  // Default Seed is the Kafka partition hashing seed.
  // https://github.com/apache/kafka/blob/trunk/.../Utils.java#L441
  uint32_t seed = 0x9747b28c);
```

The comment in the source confirms: **same key produces same partition in both Kafka and Redpanda** — full API compatibility.

Partition assignment: `murmur2(message_key) % partition_count = partition_number`

| Pros | Cons |
|---|---|
| Kafka-compatible — same key → same partition | Hash collision possible (rare) |
| Near-uniform distribution with varied keys | Skewed keys cause hot partition problem |
| Deterministic and fast | No awareness of partition load |

**Experiment 3 proves this:** Fixed key `b'same_key'` sent 100% of 3000 messages to Partition 1. Varied keys gave 32.4% / 33.0% / 34.5% distribution.

---

## 4. Concept Mapping

### 4.1 Partitioning

Redpanda's partitioning is not just data splitting — it is CPU isolation. Each partition is permanently mapped to one CPU shard via `shard_table.h:47`. `produce.cc:259` routes every incoming message to the correct core before any processing begins. This makes partition count a CPU scheduling decision, not just a throughput knob.

### 4.2 Replication

Raft consensus (`consensus.cc`) handles replication. Unlike Kafka's ISR (In-Sync Replicas) model which relies on ZooKeeper for coordination, Redpanda's Raft is self-contained. The leader sends `append_entries` RPCs to followers (`consensus.cc:738`), waits for a quorum to confirm (`consensus.cc:593`), and only then unblocks the producer (`partition.cc:383`). No external coordinator needed.

### 4.3 Fault Tolerance

Five fault tolerance mechanisms visible in the code:

1. **Raft leader election** — `_vote_timeout` fires at `consensus.cc:162`, `dispatch_vote()` starts election
2. **Leader step-down** — `do_step_down("heartbeats_majority")` at `consensus.cc:244`
3. **Write guard** — `hold_writes_enabled()` at `partition.cc:343` prevents writes during unsafe states
4. **Not-leader rejection** — `errc::not_leader` at `consensus.cc:689` redirects producers
5. **Quorum guarantee** — `replicate_finished` at `partition.cc:383` only resolves after majority ack

### 4.4 Storage

Redpanda's storage is a three-layer stack: `disk_log_impl.cc` manages the log structure, `disk_log_appender.cc` handles batch-to-segment handoff, and `segment_appender.cc` performs the actual DMA write. The `O_DIRECT` flag (expressed through `disk_write_dma_alignment()`) bypasses the OS page cache entirely — unlike Kafka which relies on the page cache for performance.

### 4.5 Streaming / Ingestion

`produce.cc` implements the Kafka produce protocol. The `fetch.cc` file implements the consumer pull model. The entire path from `produce_handler::handle()` to `dma_write()` is a streaming ingestion pipeline — messages flow through 4 distinct layers without any shared mutable state between cores.

---

## 5. Experiments

All experiments run Python scripts against a live Redpanda broker. No source code was modified — experiments observe behaviour defined by the code traced above.

### Experiment 1: Partition Count vs Throughput

**What was tested:** 5000 x 1KB messages sent with varying partition counts (1, 3, 6, 12) against a single-node broker.

**Script:** `experiments/exp1_partition_throughput.py`

**Key logic:**

```python
for partition_count in [1, 3, 6, 12]:
    # Create topic with this partition count
    subprocess.run(['rpk', 'topic', 'create', topic,
                   '--partitions', str(partition_count), ...])

    producer = KafkaProducer(bootstrap_servers=BROKER,
                             batch_size=65536, linger_ms=5, acks=1)
    # Send 5000 x 1KB messages and measure time
```

**Results:**

| Partitions | Time (s) | Msgs/sec | MB/sec |
|------------|----------|----------|--------|
| 1          | 1.19     | 4196     | 4.10   |
| **3**      | **0.95** | **5263** | **5.14** |
| 6          | 1.36     | 3679     | 3.59   |
| 12         | 1.36     | 3685     | 3.60   |

**Analysis:**
- Throughput increased **+25%** from 1 → 3 partitions
- Throughput dropped **-30%** from 3 → 6 partitions  
- 6 and 12 partitions produced **identical** results — plateau confirmed
- 8 CPU cores available (`nproc = 8`) but plateau hit at 3 partitions
- The Python `kafka-python` client became the bottleneck before the broker
- In production with a Java/C++ client, the plateau would shift right

**Code connection:** `shard_table.h:47` — each partition maps to one shard. More partitions = more shards utilized until client saturates first.

---

### Experiment 2: Partition Leader Failover

**What was tested:** 150 messages produced to a 3-node cluster (`acks=all`, `retries=10`). Leader killed at message 30 via `docker stop redpanda-2`.

**Script:** `experiments/exp2_leader_failover.py`

**Key logic:**

```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092,localhost:9093,localhost:9094',
    acks='all',       # wait for full Raft quorum
    retries=10,
    retry_backoff_ms=300
)

# Kill leader after 5 seconds
subprocess.run(['docker', 'stop', 'LEADER_CONTAINER'])
```

**Results:**

| Metric | Value |
|---|---|
| Total messages attempted | 150 |
| Successful | 150 |
| Errors during failover | **0** |
| Normal latency | ~2-4ms |
| Failover latency spike | **7230.2ms at message 33** |
| Recovery latency (message 34) | 2.3ms |
| Data loss | **Zero** |

**Message-level view (around failover):**
```
[ 30] OK  |     2.7ms    ← last message before kill
>>> KILLING redpanda-2 NOW <<<
[ 31] OK  |     1.9ms    ← in-flight, already replicated
[ 32] OK  |     1.7ms
[ 33] OK  |  7230.2ms    ← RAFT ELECTION WINDOW
[ 34] OK  |     2.3ms    ← new leader serving, instant recovery
[ 35] OK  |     2.0ms
```

**Analysis:**
- Message 33's 7.2 second spike is exactly the Raft election window
- `_vote_timeout` at `consensus.cc:162` fired when `redpanda-2` stopped responding
- New leader elected among `redpanda-1` and `redpanda-3`
- Zero errors because `acks=all` + `retries=10` held the write during election
- `replicate_finished` at `partition.cc:383` only resolved after new leader confirmed

**Code connection:** `consensus.cc:162` → `dispatch_vote()` → election → `consensus.cc:121` `_leader_notification` fires → producers transparently reconnect.

---

### Experiment 3: Partition Skew

**What was tested:** 3000 x 1KB messages sent with two key strategies — fixed key (all same partition) vs varied keys (spread across partitions).

**Script:** `experiments/exp3_partition_skew.py`

**Key logic:**

```python
# Skewed — same key every time
key = b'same_key'        # murmur2('same_key') % 3 = always Partition 1

# Even — different key every time
key = f'key_{i}'.encode()  # murmur2 distributes uniformly
```

**Results:**

| Scenario | P0 | P1 | P2 | Msgs/sec |
|---|---|---|---|---|
| Skewed (fixed key) | 0 (0%) | **3000 (100%)** | 0 (0%) | 3754 |
| Even (varied keys) | 973 (32.4%) | 991 (33.0%) | 1036 (34.5%) | 4107 |

**Throughput comparison:**
```
Skewed:  3754 msgs/sec  ████████████████████████████████████
Even  :  4107 msgs/sec  █████████████████████████████████████████
Gain  :  +9.4% from even distribution
```

**Analysis:**
- `murmur2(b'same_key') % 3` always produces Partition 1 — 100% deterministic
- One Seastar shard handled all 3000 messages while the other two sat completely idle
- `shard_table.h` maps Partition 1 to exactly one CPU core — no rebalancing possible
- Even with 8 available cores, skew means 7 cores are idle during production
- 9.4% throughput loss from skew — at production scale (millions msgs/sec) this is critical

**Code connection:** `murmur.h` — `murmur2()` with seed `0x9747b28c`. Same key always hashes to same value. `produce.cc:259` routes that partition to one fixed shard — thread-per-core cannot rebalance.

---

## 6. Failure Analysis

### Failure 1: What happens when data size increases significantly across partitions?

Each partition's segment files grow until the configured retention limit is hit. Compaction triggers to reclaim space. Large partitions increase Raft replication cost — each `append_entries` RPC carries more data per call, increasing network pressure between leader and followers.

**What the code does:**
- `segment_appender.cc:76-77` enforces flush before segment close — no data loss
- `disk_log_impl.cc` handles segment rolling when size threshold is reached
- `bytes_written` tracked at `segment_appender.cc:654` per DMA operation

**What breaks:** At very large partition sizes, the DMA write at `segment_appender.cc:648` must allocate aligned buffers for the full batch. Memory pressure increases. Raft replication RTT grows with payload size, extending the acknowledgment latency at `partition.cc:383`.

---

### Failure 2: What happens under partition skew?

Proven experimentally in Experiment 3: fixed keys cause 100% of traffic to route to one partition. The Seastar shard owning that partition becomes CPU-saturated. Other shards sit completely idle. Thread-per-core has no mechanism to rebalance — `shard_table.h` assignments are static.

**What breaks:** Producer latency spikes for the hot partition. In extreme cases the hot shard cannot drain its queue fast enough, causing backpressure at `produce.cc:302` where messages are dispatched. The 9.4% throughput loss in Experiment 3 would scale to millions of messages per second in production.

**Fix:** Use null keys for round-robin assignment (`key=None`), or redesign the key space to distribute load.

---

### Failure 3: What happens if a partition leader crashes?

Proven experimentally in Experiment 2:

1. `_vote_timeout` at `consensus.cc:162` fires when follower stops receiving heartbeats
2. `dispatch_vote(false)` called — election begins
3. `_leader_id = std::nullopt` at `consensus.cc:246` — leader cleared
4. `trigger_leadership_notification()` at `consensus.cc:247` — system notified
5. Remaining nodes elect new leader via Raft vote
6. New leader begins serving at `consensus.cc:216` (`is_elected_leader()` check)
7. Uncommitted writes (not yet quorum-acked) are rolled back — no data corruption
8. Committed writes (quorum-acked via `consensus.cc:593`) survive guaranteed

**Observed:** 7230.2ms unavailability window at message 33. Zero data loss. Zero errors with `acks=all` and `retries=10`.

---

### Failure 4: What underlying assumptions does Redpanda's partitioning rely on?

| Assumption | What breaks if violated |
|---|---|
| Keys are well-distributed | Partition skew — one shard saturated (Exp 3) |
| Partition count ≤ optimal for client | Throughput plateau then degradation (Exp 1) |
| Network latency between replicas is low | Raft election window extends beyond 7.2s |
| Disk supports fast sequential DMA writes | `dma_write()` at `segment_appender.cc:648` blocks |
| Producers use `acks=all` with retries | Data loss during leader failover without retries |
| Shard assignments are stable | Reassignment during rebalancing causes latency spikes |

---

## 7. Key Insights

1. **Partition count is a CPU scheduling decision, not just a throughput knob.**  
   Each partition maps to one CPU core via `shard_table.h`. Adding partitions beyond the optimal point adds overhead without parallelism gain — proven experimentally at 3 partitions peak despite 8 available cores.

2. **Raft guarantees zero data loss with a measurable and bounded failover window.**  
   The 7.2 second election window is Raft working correctly — not a failure. With `acks=all` and retries, producers transparently survive leader crashes with zero errors.

3. **Key design is the single most critical operational decision.**  
   `murmur2` with the same key always produces the same partition. Bad key design causes 100% skew to one shard while others are idle — observed as a 9.4% throughput loss that would be catastrophic at production scale.

4. **DMA writes are a fundamental design choice, not an optimization.**  
   By bypassing the OS page cache entirely (`segment_appender.cc:648`), Redpanda trades the unpredictable latency of cache eviction for the predictable latency of direct disk access. This is why Redpanda has lower tail latency than Kafka for the same hardware.

5. **Thread-per-core has no automatic rebalancing — this is by design.**  
   The absence of locks means the absence of coordination. Redpanda accepts static partition-to-shard assignment as a trade-off for zero contention. The operator is responsible for key distribution and partition planning.

---

## 8. How to Reproduce

### Single Node (Experiments 1 and 3)

```bash
# Start Redpanda
sudo rpk redpanda start \
  --overprovisioned --smp 1 --memory 1G \
  --reserve-memory 0M --node-id 0 \
  --check=false --install-dir /opt/redpanda &

sleep 10
rpk cluster info --brokers localhost:9092

# Run experiments
python3 experiments/exp1_partition_throughput.py
python3 experiments/exp3_partition_skew.py
```

### 3-Node Cluster (Experiment 2)

```bash
# Stop single node first
sudo pkill redpanda && sleep 3

# Start 3-node cluster
cd experiments/
docker-compose up -d
sleep 15
rpk cluster info --brokers localhost:9092

# Create replicated topic and run experiment
rpk topic create failover-test --partitions 1 --replicas 3
python3 experiments/exp2_leader_failover.py
```

---

## Repository Structure

```
redpanda-partitioning/
├── README.md                         ← This report
├── week1-notes.md                    ← Code trace notes from Week 1
├── report/
│   ├── 1_introduction.md             ← Project introduction
│   ├── 2_system_design.md            ← Design decisions with code references
│   ├── 3_observation.md              ← Experiment results and analysis
│   └── 4_failure_analysis.md         ← Failure scenarios and answers
└── experiments/
    ├── docker-compose.yml            ← 3-node cluster setup
    ├── exp1_partition_throughput.py  ← Partition count vs throughput
    ├── exp2_leader_failover.py       ← Raft leader failover
    ├── exp3_partition_skew.py        ← Fixed vs varied key distribution
    └── results/
        ├── exp1_output.txt           ← Raw output from Experiment 1
        ├── exp2_output.txt           ← Raw output from Experiment 2
        └── exp3_output.txt           ← Raw output from Experiment 3
```

---

## References

- [Redpanda GitHub Source](https://github.com/redpanda-data/redpanda)
- [Redpanda Architecture Docs](https://docs.redpanda.com/current/reference/architecture/)
- [Raft Consensus Algorithm — Ongaro & Ousterhout](https://raft.github.io/)
- [Seastar Framework](https://seastar.io/)
- [MurmurHash — aappleby/smhasher](https://github.com/aappleby/smhasher)
- [Kafka Partitioning (for comparison)](https://kafka.apache.org/documentation/)
