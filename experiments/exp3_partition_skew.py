from kafka import KafkaProducer
from collections import defaultdict
import time
import subprocess

BROKER = 'localhost:9092'
MESSAGE_COUNT = 3000
MESSAGE_SIZE = 1024
message = b'x' * MESSAGE_SIZE

def create_topic(topic, partitions):
    subprocess.run(
        ['rpk', 'topic', 'delete', topic, '--brokers', BROKER],
        capture_output=True
    )
    time.sleep(1)
    subprocess.run(
        ['rpk', 'topic', 'create', topic,
         '--partitions', str(partitions),
         '--replicas', '1',
         '--brokers', BROKER],
        capture_output=True
    )
    time.sleep(2)

def run_scenario(topic, use_fixed_key, label):
    producer = KafkaProducer(bootstrap_servers=BROKER, acks=1)
    partition_hits = defaultdict(int)

    print(f"=== {label} ===")
    start = time.time()

    futures = []
    for i in range(MESSAGE_COUNT):
        key = b'same_key' if use_fixed_key else f'key_{i}'.encode()
        future = producer.send(topic, key=key, value=message)
        futures.append(future)

    for i, f in enumerate(futures):
        result = f.get(timeout=15)
        partition_hits[result.partition] += 1

    elapsed = time.time() - start
    throughput = MESSAGE_COUNT / elapsed

    producer.close()

    print(f"Time       : {elapsed:.2f}s")
    print(f"Msgs/sec   : {throughput:.0f}")
    print(f"Distribution:")
    for p in sorted(partition_hits.keys()):
        count = partition_hits[p]
        pct = (count / MESSAGE_COUNT) * 100
        bar = '█' * (count // 30)
        print(f"  Partition {p}: {count:5d} msgs ({pct:5.1f}%)  {bar}")
    print()

    return elapsed, throughput, dict(partition_hits)

# Create topics
create_topic('skew-test', 3)
create_topic('even-test', 3)

# Run skewed scenario
elapsed_skew, tput_skew, dist_skew = run_scenario(
    'skew-test', use_fixed_key=True,
    label='SCENARIO A: SKEWED (fixed key → same partition)'
)

time.sleep(2)

# Run even scenario
elapsed_even, tput_even, dist_even = run_scenario(
    'even-test', use_fixed_key=False,
    label='SCENARIO B: EVEN (varied keys → spread across partitions)'
)

# Comparison
print("=== COMPARISON SUMMARY ===")
print(f"{'Scenario':<10} {'Time(s)':<10} {'Msgs/sec':<12} {'Hot Partition':<15}")
print("-" * 50)
hot_skew = max(dist_skew, key=dist_skew.get)
hot_even = max(dist_even, key=dist_even.get)
print(f"{'Skewed':<10} {elapsed_skew:<10.2f} {tput_skew:<12.0f} "
      f"P{hot_skew}: {dist_skew[hot_skew]} msgs")
print(f"{'Even':<10} {elapsed_even:<10.2f} {tput_even:<12.0f} "
      f"P{hot_even}: {dist_even[hot_even]} msgs")

diff = ((tput_even - tput_skew) / tput_skew) * 100
print(f"\nThroughput gain from even distribution: {diff:.1f}%")
print("\nKey Insight:")
print("Skewed keys → one Seastar shard handles everything")
print("Even keys   → work spread across all shards")
print("Thread-per-core cannot rebalance automatically")
