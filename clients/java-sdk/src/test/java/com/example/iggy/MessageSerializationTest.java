package com.example.iggy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageSerializationTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Test
    void payloadSerializesRequiredFields() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("id", 1);
        payload.put("text", "hello from Java SDK producer");
        payload.put("ts", Instant.now().toString());

        String json = objectMapper.writeValueAsString(payload);

        assertNotNull(json);
        assertTrue(json.contains("\"id\":1"));
        assertTrue(json.contains("hello from Java SDK producer"));
        assertTrue(json.contains("\"ts\""));
    }

    @Test
    void payloadDeserializesCorrectly() throws Exception {
        String json = "{\"id\":2,\"text\":\"hello from Java SDK producer\",\"ts\":\"2024-01-01T00:00:00Z\"}";

        @SuppressWarnings("unchecked")
        Map<String, Object> result = objectMapper.readValue(json, Map.class);

        assertEquals(2, result.get("id"));
        assertEquals("hello from Java SDK producer", result.get("text"));
        assertEquals("2024-01-01T00:00:00Z", result.get("ts"));
    }

    @Test
    void multiplePayloadsHaveDistinctIds() throws Exception {
        for (int i = 1; i <= 3; i++) {
            Map<String, Object> payload = new HashMap<>();
            payload.put("id", i);
            payload.put("text", "hello from Java SDK producer");
            payload.put("ts", Instant.now().toString());

            String json = objectMapper.writeValueAsString(payload);
            assertTrue(json.contains("\"id\":" + i));
        }
    }
}
