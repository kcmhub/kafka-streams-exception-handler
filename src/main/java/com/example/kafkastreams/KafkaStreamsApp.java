package com.example.kafkastreams;

import com.example.kafkastreams.handler.DlqDeserializationExceptionHandler;
import com.example.kafkastreams.handler.DlqProcessingExceptionHandler;
import com.example.kafkastreams.handler.DlqProductionExceptionHandler;
import com.example.kafkastreams.topology.OrderTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Main entry point for the Kafka Streams Order Processing application.
 * <p>
 * Demonstrates handling of three categories of exceptions, each backed by a dedicated
 * DLQ handler that routes failed records to the {@code orders-dlq} topic:
 * <ul>
 *   <li><b>Deserialization errors</b> – {@link DlqDeserializationExceptionHandler}</li>
 *   <li><b>Production / serialization errors</b> – {@link DlqProductionExceptionHandler}</li>
 *   <li><b>Processing errors (KIP-1034)</b> – {@link DlqProcessingExceptionHandler}</li>
 * </ul>
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
     * Exposed as a static method so that it can be reused / extended in tests.
     */
    public static Properties buildProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-processing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                DlqDeserializationExceptionHandler.class);
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                DlqProductionExceptionHandler.class);
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
                DlqProcessingExceptionHandler.class);
        return props;
    }
}
