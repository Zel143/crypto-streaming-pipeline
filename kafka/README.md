# Kafka setup (Ranzel's machine)

Kafka 4.3.1, KRaft mode, single node. Binaries reused from the earlier Kafka activity;
this project has its own config and storage so old topics don't show up.

| What | Path |
|---|---|
| Kafka binaries | `~/kafka-activity/kafka_2.13-4.3.1/` |
| Project config | `~/crypto-pipeline-kafka/server.properties` (copy of the default, only `log.dirs` changed) |
| Data (survives reboot) | `~/crypto-pipeline-kafka/kraft-logs/` |
| Cluster id | `~/crypto-pipeline-kafka/cluster.uuid` |
| Broker log | `~/crypto-pipeline-kafka/broker.log` |

Heap is capped by `KAFKA_HEAP_OPTS="-Xms512m -Xmx512m"` in `~/.bashrc`.

## Start the broker (own terminal tab, leave it running)
```bash
K=~/kafka-activity/kafka_2.13-4.3.1
$K/bin/kafka-server-start.sh ~/crypto-pipeline-kafka/server.properties
```
Ready when the log prints `Kafka Server started`. Stop with Ctrl+C.

## Create the two topics (once, after names are final in contract.md)
```bash
K=~/kafka-activity/kafka_2.13-4.3.1
$K/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic <TOPIC_1> --partitions 1 --replication-factor 1
$K/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic <TOPIC_2> --partitions 1 --replication-factor 1
```

## Verify (proof screenshots)
```bash
$K/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
$K/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic <TOPIC_1>
$K/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <TOPIC_1> --from-beginning --max-messages 5
```

## If storage ever needs a reset (wipes all topics)
```bash
rm -rf ~/crypto-pipeline-kafka/kraft-logs
$K/bin/kafka-storage.sh format --standalone -t $(cat ~/crypto-pipeline-kafka/cluster.uuid) -c ~/crypto-pipeline-kafka/server.properties
```

## Setup log
- 2026-09-30: config + storage created, broker started, smoke test passed (create topic, produce 1 JSON message, consume it, delete topic).
- Topic naming: Kafka warns that names mixing `.` and `_` can collide in metrics. Use one or the other (e.g. `pipeline.market-stats` or `pipeline_market_stats`), not `pipeline.market_stats`.
- 2026-10-10: created `pipeline.prices` and `pipeline.market-stats` (1 partition, RF 1). Kafka still prints the `.`/`_` warning for any name containing a `.`; it is harmless here because neither name mixes the two.
