package com.example.iggy;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Avro serialization (via {@link Schemas}) and the
 * {@link AvroConsumer#handleMessage} helper.
 */
class AvroSerializationTest {

    @Test
    void roundtripProducesOriginalRecord() throws Exception {
        byte[] bytes = Schemas.avroSerialize(1, "hello from Java SDK Avro producer", "2024-01-01T00:00:00Z");
        GenericRecord record = Schemas.avroDeserialize(bytes);

        assertEquals(1, record.get("id"));
        assertEquals("hello from Java SDK Avro producer", record.get("text").toString());
        assertEquals("2024-01-01T00:00:00Z", record.get("ts").toString());
    }

    @Test
    void serializedBytesAreNotEmpty() throws Exception {
        byte[] bytes = Schemas.avroSerialize(42, "test", "2024-01-01T00:00:00Z");
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
    }

    @Test
    void differentRecordsProduceDifferentBytes() throws Exception {
        byte[] a = Schemas.avroSerialize(1, "first",  "2024-01-01T00:00:00Z");
        byte[] b = Schemas.avroSerialize(2, "second", "2024-01-01T00:00:00Z");
        assertFalse(java.util.Arrays.equals(a, b));
    }

    @Test
    void successiveRecordsCarryIncrementingIds() throws Exception {
        for (int i = 1; i <= 3; i++) {
            byte[] bytes = Schemas.avroSerialize(i, "msg", "2024-01-01T00:00:00Z");
            GenericRecord record = Schemas.avroDeserialize(bytes);
            assertEquals(i, record.get("id"));
        }
    }
}
