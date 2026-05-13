from kafka import KafkaProducer
import time
import subprocess

BROKER = 'localhost:9092'
MESSAGE_COUNT = 5000
MESSAGE_SIZE = 1024
message = b'x' * MESSAGE_SIZE

results = []

print("=== EXPERIMENT 1: PARTITION COUNT VS THROUGHPUT ===\n")

for partition_count in [1, 3, 6, 12]:

    topic = f'bench-{partition_count}p'

    # Delete topic if exists then recreate
    subprocess.run(
        ['rpk', 'topic', 'delete', topic, '--brokers', BROKER],
        capture_output=True
    )
    time.sleep(1)
    subprocess.run(
        ['rpk', 'topic', 'create', topic,
         '--partitions', str(partition_count),
         '--replicas', '1',
         '--brokers', BROKER],
        capture_output=True
    )
    time.sleep(2)

    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        batch_size=65536,
        linger_ms=5,
        acks=1
    )

    print(f"Testing with {partition_count} partition(s)...")
    start = time.time()

    futures = []
    for i in range(MESSAGE_COUNT):
        key = f'key_{i}'.encode()
        futures.append(
            producer.send(topic, key=key, value=message)
        )

    for f in futures:
        f.get(timeout=30)

    elapsed = time.time() - start
    throughput = MESSAGE_COUNT / elapsed
    mb_per_sec = (MESSAGE_COUNT * MESSAGE_SIZE) / (elapsed * 1024 * 1024)

    results.append((partition_count, elapsed, throughput, mb_per_sec))
    producer.close()

    print(f"  Partitions : {partition_count}")
    print(f"  Time       : {elapsed:.2f}s")
    print(f"  Msgs/sec   : {throughput:.0f}")
    print(f"  MB/sec     : {mb_per_sec:.2f}")
    print()

print("=== SUMMARY ===")
print(f"{'Partitions':<12} {'Time(s)':<10} {'Msgs/sec':<12} {'MB/sec':<10}")
print("-" * 45)
for r in results:
    print(f"{r[0]:<12} {r[1]:<10.2f} {r[2]:<12.0f} {r[3]:<10.2f}")
