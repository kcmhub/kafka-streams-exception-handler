package com.example.kafkastreams.topology;

import com.example.kafkastreams.model.Order;
import com.example.kafkastreams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the Kafka Streams topology for the Order Processing application.
 * <p>
 * Flow: {@code orders-input} → validate &amp; enrich → {@code orders-output}
 * <p>
 * Invalid orders (null/blank orderId or non-positive amount) throw an
 * {@link IllegalArgumentException} in {@code mapValues}, which is caught by the
 * configured {@link org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler}
 * and routed to {@code orders-dlq}.
 */
public class OrderTopology {

    private static final Logger logger = LoggerFactory.getLogger(OrderTopology.class);

    public static final String INPUT_TOPIC  = "orders-input";
    public static final String OUTPUT_TOPIC = "orders-output";
    public static final String DLQ_TOPIC    = "orders-dlq";

    private final JsonSerde<Order> orderSerde;

    public OrderTopology() {
        this.orderSerde = new JsonSerde<>(Order.class);
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), orderSerde))
                .mapValues(order -> {
                    logger.debug("Processing order: {}", order);
                    validateOrder(order);
                    if (order.getDescription() != null) {
                        order.setDescription(order.getDescription().toUpperCase());
                    }
                    logger.debug("Successfully processed order: {}", order);
                    return order;
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), orderSerde));

        return builder.build();
    }

    private void validateOrder(Order order) {
        if (order.getOrderId() == null || order.getOrderId().isBlank()) {
            throw new IllegalArgumentException("orderId must not be null or blank");
        }
        if (order.getAmount() <= 0) {
            throw new IllegalArgumentException(
                    "amount must be positive, got: " + order.getAmount());
        }
    }
}
