package com.example.kafkastreams;

import com.example.kafkastreams.topology.OrderTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Main entry point for the Kafka Streams Order Processing application.
 * <p>
 * Demonstrates the built-in Dead Letter Queue (DLQ) support introduced in
 * Kafka Streams 4.0+ (KIP-1034). All three exception categories are handled by
 * the Kafka Streams built-in handlers, which are configured with a single
 * property ({@code errors.dead.letter.queue.topic.name}):
 * <ul>
 *   <li><b>Deserialization errors</b> –
 *       {@link LogAndContinueExceptionHandler}: logs the error, routes raw bytes
 *       to the DLQ topic, and resumes processing.</li>
 *   <li><b>Processing errors</b> (KIP-1034) –
 *       {@link LogAndContinueProcessingExceptionHandler}: logs the error, routes
 *       raw bytes to the DLQ topic, and resumes processing.</li>
 *   <li><b>Production / serialization errors</b> –
 *       {@link DefaultProductionExceptionHandler}: retries on transient errors;
 *       for all other failures routes raw bytes to the DLQ topic then fails the
 *       stream thread (production failures are considered fatal).</li>
 * </ul>
 * Each DLQ record is enriched with standard error headers by the framework:
 * {@code __streams.errors.exception}, {@code __streams.errors.message},
 * {@code __streams.errors.stacktrace}, {@code __streams.errors.topic},
 * {@code __streams.errors.partition}, and {@code __streams.errors.offset}.
 */
public class KafkaStreamsApp {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApp.class);

    public static void main(String[] args) throws InterruptedException {
        Properties props = buildProperties();
        Topology topology = new OrderTopology().buildTopology();

        logger.info("Topology description:\n{}", topology.describe());

        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            CountDownLatch latch = new CountDownLatch(1);

            streams.setUncaughtExceptionHandler(exception -> {
                logger.error("Uncaught exception in stream thread — stopping", exception);
                latch.countDown();
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down Kafka Streams application...");
                streams.close();
                latch.countDown();
            }, "streams-shutdown-hook"));

            streams.start();
            logger.info("Kafka Streams application started. Listening on topic '{}'.",
                    OrderTopology.INPUT_TOPIC);
            latch.await();
        }
    }

    /**
     * Builds the {@link Properties} used to configure the {@link KafkaStreams} instance.
     * Exposed as a static method so that it can be reused in tests.
     */
    public static Properties buildProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-processing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Built-in DLQ support: route all failed records to the DLQ topic.
        // A single config key activates DLQ routing in all three built-in handlers.
        props.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, OrderTopology.DLQ_TOPIC);

        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueProcessingExceptionHandler.class);
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                DefaultProductionExceptionHandler.class);
        return props;
    }
}
