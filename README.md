# kafka-streams-exception-handler

A complete Kafka Streams 4.2.0 example that demonstrates robust exception handling for all three
error categories using the built-in Dead Letter Queue (DLQ) API introduced in KIP-1034.

---

## Overview

| Exception type | Triggered by | Handler class |
|---|---|---|
| **Deserialization** | Bytes that cannot be parsed into the expected type | `DlqDeserializationExceptionHandler` |
| **Processing** (KIP-1034) | Exception thrown inside `mapValues`, `filter`, etc. | `DlqProcessingExceptionHandler` |
| **Production / Serialization** | Failure to write or serialize a record to Kafka | `DlqProductionExceptionHandler` |

When any of the above errors occurs, the failing record's **raw bytes** are forwarded to the
`orders-dlq` topic together with diagnostic headers (`error.type`, `error.exception.class`,
`error.exception.message`, `error.source.topic`, …). Stream processing then continues with
the next record rather than crashing the application.

---

## Architecture

```
orders-input  ──► [deserialize] ──► mapValues (validate + enrich) ──► orders-output
                       │                         │
                  DeserializationEx          ProcessingEx
                  Handler (DLQ)              Handler (DLQ)
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

The application logs a deserialization error and the raw bytes appear on `orders-dlq`:

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

The record does NOT appear in `orders-output`; it lands in `orders-dlq` with
`error.type=PROCESSING` and `error.exception.message=amount must be positive, got: -1.0`.

### 7 — Serialization error

Serialization errors are triggered when the configured `Serializer` fails to convert an
in-memory object to bytes before writing to the output topic. This typically happens in
production when a custom serializer encounters an object it cannot handle.  In this project
the `JsonSerializer` will throw a `RuntimeException` if Jackson cannot marshal the object.
These errors are caught by `DlqProductionExceptionHandler.handleSerializationError()`, which
routes the raw source bytes to the DLQ with `error.type=PRODUCTION` and
`error.serialization.origin=KEY` or `VALUE`.

### 8 — Stop everything

```bash
# Ctrl+C to stop the app, then:
docker-compose down
```

---

## Exception Handler Summary

| Class | Interface | Config key |
|---|---|---|
| `DlqDeserializationExceptionHandler` | `DeserializationExceptionHandler` | `DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG` |
| `DlqProcessingExceptionHandler` | `ProcessingExceptionHandler` (KIP-1034) | `PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG` |
| `DlqProductionExceptionHandler` | `ProductionExceptionHandler` | `DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG` |

All handlers produce `ProducerRecord<byte[], byte[]>` records to `orders-dlq` enriched with
the following headers:

| Header | Description |
|---|---|
| `error.type` | `DESERIALIZATION`, `PROCESSING`, or `PRODUCTION` |
| `error.exception.class` | Fully-qualified exception class name |
| `error.exception.message` | Exception message (if present) |
| `error.source.topic` | Source topic of the failed record |
| `error.source.partition` | Source partition |
| `error.source.offset` | Source offset |
| `error.processor.node` | Processor node ID (processing errors only) |
| `error.serialization.origin` | `KEY` or `VALUE` (serialization errors only) |
