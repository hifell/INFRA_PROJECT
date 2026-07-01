from prometheus_client import Counter, Gauge

kafka_messages_total = Counter(
    "pipeline_kafka_messages_total",
    "Total pesan dari Kafka",
    ["token"]
)

cassandra_rows_total = Counter(
    "pipeline_cassandra_rows_total",
    "Total rows Cassandra",
    ["token"]
)

pipeline_errors_total = Counter(
    "pipeline_errors_total",
    "Total error pipeline",
    ["component"]
)