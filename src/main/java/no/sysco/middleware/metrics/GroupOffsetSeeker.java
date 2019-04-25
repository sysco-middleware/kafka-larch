package no.sysco.middleware.metrics;

import io.prometheus.client.Gauge;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class GroupOffsetSeeker implements AutoCloseable {

    public static Logger logger = LoggerFactory.getLogger(GroupOffsetSeeker.class);


    static final Gauge lagGauge = Gauge.build()
        .name("kafka_larch_offset_lag")
        .help("Lag between the partition's end offset and consumer's current offset")
        .labelNames("consumer_group", "topic", "partition")
        .register();


    private final String bootstrapServers;
    private final Map<String, Object> kafkaConsumerConf;
    private final List<String> groupIds;
    private final long queryIntervalMs;
    private final long queryTimeoutMs;

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    ExecutorService consumerExecService = Executors.newSingleThreadExecutor();
    private AdminClient adminClient;

    public GroupOffsetSeeker(final String bootstrapServers,
                             final Map<String, Object> kafkaConsumerConf,
                             final String groupsCsv,
                             final long queryIntervalMs,
                             final long queryTimeoutMs) {
        this.bootstrapServers = bootstrapServers;
        this.kafkaConsumerConf = kafkaConsumerConf;
        this.groupIds = loadGroupsToMonitor(groupsCsv);
        this.queryIntervalMs = queryIntervalMs;
        this.queryTimeoutMs = queryTimeoutMs;
    }

    /**
     * Split the input string by comma and remove any whitespaces around the comma
     * @param groups comma-separated string of groups to monitor
     * @return list of groupIds that should be monitored
     */
    private List<String> loadGroupsToMonitor(final String groups) {
        List<String> groupIds = new ArrayList<>(Arrays.asList(groups.split("\\s*,\\s*")));
        groupIds.removeAll(Arrays.asList("", null));
        return groupIds;
    }

    private void collectGroupOffset(String groupId, long queryTimeoutMs) {
        ListConsumerGroupOffsetsResult res = adminClient.listConsumerGroupOffsets(groupId);

        try {
            Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap =
                res.partitionsToOffsetAndMetadata().get(queryTimeoutMs, TimeUnit.MILLISECONDS);
            Set<TopicPartition> topicPartitions = topicPartitionOffsetAndMetadataMap.keySet();

            Map<TopicPartition, Long> endOffsetsPerPartition =
                kafkaConsumer.endOffsets(topicPartitions, Duration.of(queryTimeoutMs, ChronoUnit.MILLIS));

            for (TopicPartition partition : topicPartitions) {
                OffsetAndMetadata offsetAndMetadata = topicPartitionOffsetAndMetadataMap.get(partition);
                long currentOffset = offsetAndMetadata.offset();

                Long endOffset = endOffsetsPerPartition.get(partition);
                if(endOffset != null) {
                    long lag = endOffset - currentOffset;
                    lagGauge.labels(groupId, partition.topic(), Integer.toString(partition.partition())).set(lag);

                    logger.debug("group: {}, topic: {}, partition: {}, offset: {}, end offset: {},  lag: {}",
                                 groupId, partition.topic(), partition.partition(), currentOffset, endOffset, lag);
                } else {
                    logger.warn("Missing END OFFSET for group: {}, topic: {}, partition: {}",groupId, partition.topic(), partition.partition());
                }

            }

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("Error occurred while fetching partition info", e);
        }
    }

    private AdminClient createAdminClient(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(properties);
    }

    public void run() {
        adminClient = createAdminClient(bootstrapServers);;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaConsumerConf)) {
            kafkaConsumer = consumer;

            while(true) {
                for (String groupId : groupIds) {
                    collectGroupOffset(groupId, queryTimeoutMs);
                }
                Thread.sleep(queryIntervalMs);
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted", e);
        } catch (Exception e) {
            logger.error("Unexpected exception on consumer thread", e);
            System.exit(1);
        }
    }

    public void start() {
        if (kafkaConsumer == null) {
            consumerExecService.submit(this::run);
        } else {
            throw new IllegalStateException("Start was invoked after the consumer was already running");
        }
    }

    @Override
    public void close() {
        try {
            if (kafkaConsumer != null) {
                kafkaConsumer.wakeup();
            }
            consumerExecService.shutdown();
            consumerExecService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (Exception ex) {
            logger.warn("Exception during shutdown", ex);
        }
    }
}