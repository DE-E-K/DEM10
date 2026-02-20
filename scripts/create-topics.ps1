$bootstrap = $env:KAFKA_BOOTSTRAP_SERVERS
if (-not $bootstrap) { $bootstrap = "localhost:9092" }

$topics = @(
  @{ name = "events.raw.v1"; partitions = 24 },
  @{ name = "events.invalid.v1"; partitions = 6 },
  @{ name = "events.anomaly.v1"; partitions = 6 },
  @{ name = "events.dlq.v1"; partitions = 6 }
)

foreach ($t in $topics) {
  docker exec heartbeat-kafka kafka-topics --bootstrap-server $bootstrap --create --if-not-exists --topic $t.name --partitions $t.partitions --replication-factor 1
}

docker exec heartbeat-kafka kafka-topics --bootstrap-server $bootstrap --list
