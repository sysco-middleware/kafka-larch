# kafka-larch
Prometheus exporter of kafka offsets for given groups

This application periodically queries offsets of a given consumer group(s) across topics, reports the current offset and lag
as prometheus metrics.
