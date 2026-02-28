package com.example.iggy;

import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Protobuf serialization (via {@link Schemas}) and the
 * {@link ProtobufConsumer#handleMessage} helper.
 */
class ProtobufSerializationTest {

    @Test
    void roundtripRestoresAllFields() throws Exception {
        byte[] bytes = Schemas.protobufSerialize(1, "hello from Java Protobuf producer", "2024-01-01T00:00:00Z");
        DynamicMessage event = Schemas.protobufDeserialize(bytes);

        assertEquals(1, event.getField(Schemas.eventDescriptor().findFieldByName("id")));
        assertEquals("hello from Java Protobuf producer",
                event.getField(Schemas.eventDescriptor().findFieldByName("text")));
        assertEquals("2024-01-01T00:00:00Z",
                event.getField(Schemas.eventDescriptor().findFieldByName("ts")));
    }

    @Test
    void serializedBytesAreNotEmpty() {
        byte[] bytes = Schemas.protobufSerialize(42, "test", "2024-01-01T00:00:00Z");
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
    }

    @Test
    void differentIdsProduceDifferentBytes() {
        byte[] a = Schemas.protobufSerialize(1, "same text", "2024-01-01T00:00:00Z");
        byte[] b = Schemas.protobufSerialize(2, "same text", "2024-01-01T00:00:00Z");
        assertFalse(java.util.Arrays.equals(a, b));
    }

    @Test
    void successiveEventsCarryIncrementingIds() throws Exception {
        for (int i = 1; i <= 3; i++) {
            byte[] bytes = Schemas.protobufSerialize(i, "msg", "2024-01-01T00:00:00Z");
            DynamicMessage event = Schemas.protobufDeserialize(bytes);
            assertEquals(i, event.getField(Schemas.eventDescriptor().findFieldByName("id")));
        }
    }

    @Test
    void defaultValuesRoundtrip() throws Exception {
        byte[] bytes = Schemas.protobufSerialize(0, "", "");
        DynamicMessage event = Schemas.protobufDeserialize(bytes);
        assertEquals(0, event.getField(Schemas.eventDescriptor().findFieldByName("id")));
        assertEquals("", event.getField(Schemas.eventDescriptor().findFieldByName("text")));
        assertEquals("", event.getField(Schemas.eventDescriptor().findFieldByName("ts")));
    }
}
