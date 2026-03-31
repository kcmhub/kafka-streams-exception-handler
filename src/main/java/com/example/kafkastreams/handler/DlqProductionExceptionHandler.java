package com.example.kafkastreams.handler;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

/**
 * Production (write / serialization) exception handler that routes failed records to a
 * Dead Letter Queue (DLQ).
 * <p>
 * <ul>
 *   <li>For {@link RetriableException}s the handler returns {@link Response#retry()},
 *       letting Kafka Streams retry the send automatically.</li>
 *   <li>For serialization failures ({@link #handleSerializationError}) and all other
 *       non-retriable production errors the failed record is forwarded to the DLQ topic
 *       with error metadata in the headers, and processing continues.</li>
 * </ul>
 */
public class DlqProductionExceptionHandler implements ProductionExceptionHandler {

    private static final Logger logger =
            LoggerFactory.getLogger(DlqProductionExceptionHandler.class);

    private static final String DLQ_TOPIC = "orders-dlq";

    @Override
    public Response handleError(ErrorHandlerContext context,
                                ProducerRecord<byte[], byte[]> record,
                                Exception exception) {
        logger.error(
                "Production error on topic='{}' partition={} offset={}: {}",
                context.topic(), context.partition(), context.offset(),
                exception.getMessage(), exception);

        if (exception instanceof RetriableException) {
            logger.warn("Retriable production error — will retry: {}", exception.getMessage());
            return Response.retry();
        }

        ProducerRecord<byte[], byte[]> dlqRecord =
                buildDlqRecord(context, record.key(), record.value(), exception, null);
        return Response.resume(Collections.singletonList(dlqRecord));
    }

    @Override
    public Response handleSerializationError(ErrorHandlerContext context,
                                             ProducerRecord record,
                                             Exception exception,
                                             SerializationExceptionOrigin origin) {
        logger.error(
                "Serialization error ({}) on topic='{}': {}",
                origin, context.topic(), exception.getMessage(), exception);

        ProducerRecord<byte[], byte[]> dlqRecord =
                buildDlqRecord(context, context.sourceRawKey(), context.sourceRawValue(),
                        exception, origin);
        return Response.resume(Collections.singletonList(dlqRecord));
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No additional configuration required
    }

    private ProducerRecord<byte[], byte[]> buildDlqRecord(ErrorHandlerContext context,
                                                          byte[] key,
                                                          byte[] value,
                                                          Exception exception,
                                                          SerializationExceptionOrigin origin) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("error.type",
                "PRODUCTION".getBytes(StandardCharsets.UTF_8));
        headers.add("error.exception.class",
                exception.getClass().getName().getBytes(StandardCharsets.UTF_8));
        if (exception.getMessage() != null) {
            headers.add("error.exception.message",
                    exception.getMessage().getBytes(StandardCharsets.UTF_8));
        }
        headers.add("error.source.topic",
                context.topic().getBytes(StandardCharsets.UTF_8));
        if (origin != null) {
            headers.add("error.serialization.origin",
                    origin.name().getBytes(StandardCharsets.UTF_8));
        }

        return new ProducerRecord<>(DLQ_TOPIC, null, null, key, value, headers);
    }
}
