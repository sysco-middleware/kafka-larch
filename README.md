# kafka-larch
Prometheus exporter of kafka offsets for a given groups/topics

This application periodically queries offsets of a given consumer group(s) across given topics, reports the current offset and lag
as prometheus metrics.
