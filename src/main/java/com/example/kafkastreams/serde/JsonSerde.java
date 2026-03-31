package com.example.kafkastreams.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A generic Kafka {@link Serde} backed by a Jackson {@link ObjectMapper}.
 * <p>
 * The inner {@link JsonSerializer} converts objects to JSON bytes;
 * the inner {@link JsonDeserializer} converts JSON bytes back to objects.
 * {@code null} input is handled gracefully by returning {@code null}.
 *
 * @param <T> the type to serialize / deserialize
 */
public class JsonSerde<T> implements Serde<T> {

    private final JsonSerializer<T> serializer;
    private final JsonDeserializer<T> deserializer;

    public JsonSerde(Class<T> type) {
        ObjectMapper objectMapper = new ObjectMapper();
        this.serializer = new JsonSerializer<>(objectMapper);
        this.deserializer = new JsonDeserializer<>(type, objectMapper);
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }

    // -------------------------------------------------------------------------
    // Inner classes
    // -------------------------------------------------------------------------

    public static class JsonSerializer<T> implements Serializer<T> {

        private final ObjectMapper objectMapper;

        JsonSerializer(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize object to JSON for topic: " + topic, e);
            }
        }
    }

    public static class JsonDeserializer<T> implements Deserializer<T> {

        private static final Logger logger = LoggerFactory.getLogger(JsonDeserializer.class);

        private final Class<T> type;
        private final ObjectMapper objectMapper;

        JsonDeserializer(Class<T> type, ObjectMapper objectMapper) {
            this.type = type;
            this.objectMapper = objectMapper;
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.readValue(data, type);
            } catch (IOException e) {
                logger.error("Failed to deserialize bytes from topic '{}': {}", topic, e.getMessage());
                throw new RuntimeException("Failed to deserialize JSON for type " + type.getSimpleName(), e);
            }
        }
    }
}
