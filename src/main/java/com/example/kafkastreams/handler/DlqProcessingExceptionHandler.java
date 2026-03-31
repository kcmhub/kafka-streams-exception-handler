package com.example.kafkastreams.handler;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

/**
 * Processing exception handler (KIP-1034, Kafka 4.x) that routes failed records to a
 * Dead Letter Queue (DLQ).
 * <p>
 * When an exception is thrown during stream processing (inside {@code mapValues},
 * {@code filter}, etc.) the original raw bytes are forwarded to the {@code orders-dlq}
 * topic with error metadata in the record headers, and stream processing continues
 * with the next record.
 */
public class DlqProcessingExceptionHandler implements ProcessingExceptionHandler {

    private static final Logger logger =
            LoggerFactory.getLogger(DlqProcessingExceptionHandler.class);

    private static final String DLQ_TOPIC = "orders-dlq";

    @Override
    public Response handleError(ErrorHandlerContext context,
                                Record<?, ?> record,
                                Exception exception) {
        logger.error(
                "Processing error in node='{}' on topic='{}' partition={} offset={}: {}",
                context.processorNodeId(), context.topic(),
                context.partition(), context.offset(),
                exception.getMessage(), exception);

        ProducerRecord<byte[], byte[]> dlqRecord = buildDlqRecord(context, exception);
        return Response.resume(Collections.singletonList(dlqRecord));
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No additional configuration required
    }

    private ProducerRecord<byte[], byte[]> buildDlqRecord(ErrorHandlerContext context,
                                                          Exception exception) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("error.type",
                "PROCESSING".getBytes(StandardCharsets.UTF_8));
        headers.add("error.exception.class",
                exception.getClass().getName().getBytes(StandardCharsets.UTF_8));
        if (exception.getMessage() != null) {
            headers.add("error.exception.message",
                    exception.getMessage().getBytes(StandardCharsets.UTF_8));
        }
        headers.add("error.source.topic",
                context.topic().getBytes(StandardCharsets.UTF_8));
        headers.add("error.source.partition",
                String.valueOf(context.partition()).getBytes(StandardCharsets.UTF_8));
        headers.add("error.source.offset",
                String.valueOf(context.offset()).getBytes(StandardCharsets.UTF_8));
        headers.add("error.processor.node",
                context.processorNodeId().getBytes(StandardCharsets.UTF_8));

        return new ProducerRecord<>(DLQ_TOPIC, null, null,
                context.sourceRawKey(), context.sourceRawValue(), headers);
    }
}
