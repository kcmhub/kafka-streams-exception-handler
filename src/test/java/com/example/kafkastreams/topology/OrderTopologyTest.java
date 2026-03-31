package com.example.kafkastreams.topology;

import com.example.kafkastreams.handler.DlqDeserializationExceptionHandler;
import com.example.kafkastreams.handler.DlqProcessingExceptionHandler;
import com.example.kafkastreams.handler.DlqProductionExceptionHandler;
import com.example.kafkastreams.model.Order;
import com.example.kafkastreams.serde.JsonSerde;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OrderTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, Order> orderInputTopic;
    private TestOutputTopic<String, Order> orderOutputTopic;
    private TestOutputTopic<byte[], byte[]> dlqOutputTopic;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-order-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                DlqDeserializationExceptionHandler.class);
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
                DlqProcessingExceptionHandler.class);
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                DlqProductionExceptionHandler.class);

        driver = new TopologyTestDriver(new OrderTopology().buildTopology(), props);

        JsonSerde<Order> orderSerde = new JsonSerde<>(Order.class);
        orderInputTopic = driver.createInputTopic(
                OrderTopology.INPUT_TOPIC, new StringSerializer(), orderSerde.serializer());
        orderOutputTopic = driver.createOutputTopic(
                OrderTopology.OUTPUT_TOPIC, new StringDeserializer(), orderSerde.deserializer());
        dlqOutputTopic = driver.createOutputTopic(
                OrderTopology.DLQ_TOPIC, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    /**
     * Nominal case: a valid order is processed and enriched (description uppercased).
     * No record should appear in the DLQ.
     */
    @Test
    void testNominalCase() {
        Order order = new Order("order-1", "customer-1", 99.99, "test order");
        orderInputTopic.pipeInput("order-1", order);

        assertThat(orderOutputTopic.isEmpty()).isFalse();
        var output = orderOutputTopic.readRecord();
        assertThat(output.key()).isEqualTo("order-1");
        assertThat(output.value().getOrderId()).isEqualTo("order-1");
        assertThat(output.value().getAmount()).isEqualTo(99.99);
        assertThat(output.value().getDescription()).isEqualTo("TEST ORDER");
        assertThat(dlqOutputTopic.isEmpty()).isTrue();
    }

    /**
     * Deserialization error: bytes that are not valid JSON trigger the
     * {@link DlqDeserializationExceptionHandler}, which routes the raw bytes to the DLQ topic.
     */
    @Test
    void testDeserializationError() {
        byte[] invalidJson = "{not-valid-json}".getBytes(StandardCharsets.UTF_8);

        // Pipe raw bytes that will fail JSON deserialization in the topology
        TestInputTopic<String, byte[]> rawInputTopic = driver.createInputTopic(
                OrderTopology.INPUT_TOPIC, new StringSerializer(), new ByteArraySerializer());
        rawInputTopic.pipeInput("bad-key", invalidJson);

        // No record should reach the output topic
        assertThat(orderOutputTopic.isEmpty()).isTrue();

        // The raw bytes must have been routed to the DLQ
        assertThat(dlqOutputTopic.isEmpty()).isFalse();
        var dlqRecord = dlqOutputTopic.readRecord();
        assertThat(dlqRecord.value()).isEqualTo(invalidJson);
    }

    /**
     * Processing error (KIP-1034): a valid order with a negative amount passes
     * deserialization but throws in {@code mapValues}. The
     * {@link DlqProcessingExceptionHandler} routes it to the DLQ.
     */
    @Test
    void testProcessingError_negativeAmount() {
        orderInputTopic.pipeInput("order-2", new Order("order-2", "customer-2", -50.0, "negative amount"));

        assertThat(orderOutputTopic.isEmpty()).isTrue();
        assertThat(dlqOutputTopic.isEmpty()).isFalse();
    }

    /**
     * Processing error (KIP-1034): a blank orderId causes a validation failure in
     * {@code mapValues}.
     */
    @Test
    void testProcessingError_blankOrderId() {
        orderInputTopic.pipeInput("order-3", new Order("", "customer-3", 50.0, "blank order id"));

        assertThat(orderOutputTopic.isEmpty()).isTrue();
        assertThat(dlqOutputTopic.isEmpty()).isFalse();
    }

    /**
     * Mixed batch: valid orders appear in the output topic; invalid orders are silently
     * routed to the DLQ without interrupting stream processing.
     */
    @Test
    void testMixedValidAndInvalidOrders() {
        orderInputTopic.pipeInput("order-10", new Order("order-10", "customer-10", 100.0, "valid"));
        orderInputTopic.pipeInput("order-11", new Order("order-11", "customer-11", -1.0,  "bad amount"));
        orderInputTopic.pipeInput("order-12", new Order("order-12", "customer-12", 200.0, "also valid"));

        assertThat(orderOutputTopic.readRecordsToList()).hasSize(2);
        assertThat(dlqOutputTopic.readRecordsToList()).hasSize(1);
    }
}
