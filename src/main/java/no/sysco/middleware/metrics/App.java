package no.sysco.middleware.metrics;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class App {
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        logger.info("Larch app booting...");

        logger.debug("Loading config");
        Config config = ConfigFactory.load();
        Map<String, Object> kafkaConsumerConf = config.getObject("larch.kafka").unwrapped();
        String bootstrapServers = (String)kafkaConsumerConf.get("bootstrap.servers");
        String groupIds = config.getString("larch.offset-lag.groups");

        long queryIntervalMs = config.getLong("larch.offset-lag.query-interval-ms");
        long queryTimeoutMs = config.getLong("larch.offset-lag.query-timeout-ms");

        //preparing prometheus exporters
        DefaultExports.initialize();
        logger.info("Hotspot metrics collectors started.");

        final HTTPServer server;
        try {
            server = new HTTPServer(config.getInt("larch.prometheus.port"), false);
        } catch (IOException ioe) {
            logger.error("Prometheus server failed to start", ioe);
            System.exit(1);
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));

        GroupOffsetSeeker go = new GroupOffsetSeeker(bootstrapServers, kafkaConsumerConf, groupIds, queryIntervalMs, queryTimeoutMs);
        go.start();
        logger.info("Larch app running");
    }

}
