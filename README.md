# kafka-streams-exception-handler

A complete Kafka Streams 4.2.0 example that demonstrates robust exception handling for all three
error categories using the **built-in Dead Letter Queue (DLQ) support** introduced in
[KIP-1034](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1034%3A+Dead+letter+queue+in+Kafka+Streams).

No custom exception handler classes are needed. A single configuration property activates DLQ
routing across all three built-in handlers.

---

## Overview

Kafka Streams 4.0+ ships with three built-in exception handlers that natively support DLQ routing
when the `errors.dead.letter.queue.topic.name` property is set:

| Exception type | Triggered by | Built-in handler |
|---|---|---|
| **Deserialization** | Bytes that cannot be parsed into the expected type | `LogAndContinueExceptionHandler` |
| **Processing** (KIP-1034) | Exception thrown inside `mapValues`, `filter`, etc. | `LogAndContinueProcessingExceptionHandler` |
| **Production / Serialization** | Failure to write or serialize a record to Kafka | `DefaultProductionExceptionHandler` |

When any of the above errors occurs the failing record's raw bytes are forwarded to the configured
DLQ topic together with standard diagnostic headers added by the framework. Stream processing then
continues with the next record (deserialization and processing errors) or the stream thread is
stopped after DLQ routing (production errors, which are considered fatal).

### Minimal configuration

```java
props.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "orders-dlq");

props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);
props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueProcessingExceptionHandler.class);
props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
        DefaultProductionExceptionHandler.class);
```

---

## Architecture

```
orders-input  ──► [deserialize] ──► mapValues (validate + enrich) ──► orders-output
                       │                         │
                  DeserializationEx          ProcessingEx
                  (LogAndContinue)           (LogAndContinue)
                       │                         │
                       └──────────┬──────────────┘
                                  ▼
                             orders-dlq
```

**Topics**

| Topic | Key | Value | Purpose |
|---|---|---|---|
| `orders-input` | `String` | JSON `Order` | Raw input orders |
| `orders-output` | `String` | JSON `Order` | Successfully processed orders (description uppercased) |
| `orders-dlq` | `byte[]` | `byte[]` | Failed records with error headers |

**Order validation rules**

- `orderId` must not be `null` or blank.
- `amount` must be `> 0`.

---

## Prerequisites

- Java 17
- Maven 3.9+
- Docker & Docker Compose (for local end-to-end testing)

---

## Build

```bash
mvn clean package -DskipTests
```

---

## Run Unit Tests

```bash
mvn test
```

The test suite uses `TopologyTestDriver` (no running Kafka required) and covers:

| Test | Scenario |
|---|---|
| `testNominalCase` | Valid order → output with uppercased description |
| `testDeserializationError` | Invalid JSON bytes → DLQ |
| `testProcessingError_negativeAmount` | `amount = -50` → DLQ |
| `testProcessingError_blankOrderId` | Empty `orderId` → DLQ |
| `testMixedValidAndInvalidOrders` | Batch of 3 (1 bad) → 2 outputs + 1 DLQ |

---

## Local End-to-End Testing

### 1 — Start Kafka

```bash
docker-compose up -d
# Wait ~15 s for the broker to become healthy
docker-compose ps
```

### 2 — Create topics

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders-input  --partitions 1 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders-output --partitions 1 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders-dlq    --partitions 1 --replication-factor 1
```

### 3 — Start the application

```bash
mvn exec:java -Dexec.mainClass=com.example.kafkastreams.KafkaStreamsApp
```

### 4 — Nominal case (valid order)

In a second terminal, produce a valid order:

```bash
echo 'order-1:{"orderId":"order-1","customerId":"cust-1","amount":49.99,"description":"hello world"}' | \
  docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic orders-input \
    --property "parse.key=true" \
    --property "key.separator=:"
```

Consume from the output topic — the description should be uppercased:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders-output \
  --from-beginning \
  --property "print.key=true"
```

Expected output:
```
order-1  {"orderId":"order-1","customerId":"cust-1","amount":49.99,"description":"HELLO WORLD"}
```

### 5 — Deserialization error (invalid JSON)

```bash
echo 'bad-key:this-is-not-json' | \
  docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic orders-input \
    --property "parse.key=true" \
    --property "key.separator=:"
```

`LogAndContinueExceptionHandler` logs the deserialization error and the raw bytes appear on
`orders-dlq` with framework headers. Consume and inspect:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders-dlq \
  --from-beginning \
  --property "print.key=true" \
  --property "print.headers=true"
```

### 6 — Processing error (negative amount)

```bash
echo 'order-2:{"orderId":"order-2","customerId":"cust-2","amount":-1.0,"description":"bad order"}' | \
  docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic orders-input \
    --property "parse.key=true" \
    --property "key.separator=:"
```

The record does NOT appear in `orders-output`; it lands in `orders-dlq` with header
`__streams.errors.message=amount must be positive, got: -1.0`.

### 7 — Serialization error

Serialization errors are triggered when the configured `Serializer` fails to convert an
in-memory object to bytes before writing to the output topic. `DefaultProductionExceptionHandler`
catches these, routes the raw source bytes to the DLQ topic, then fails the stream thread
(production failures are considered fatal and require operator attention).

### 8 — Stop everything

```bash
# Ctrl+C to stop the app, then:
docker-compose down
```

---

## DLQ record headers

The Kafka Streams framework automatically adds the following headers to every DLQ record via
`ExceptionHandlerUtils`:

| Header | Description |
|---|---|
| `__streams.errors.exception` | Fully-qualified exception class name |
| `__streams.errors.message` | Exception message |
| `__streams.errors.stacktrace` | Full stack trace |
| `__streams.errors.topic` | Source topic of the failed record |
| `__streams.errors.partition` | Source partition |
| `__streams.errors.offset` | Source offset |
