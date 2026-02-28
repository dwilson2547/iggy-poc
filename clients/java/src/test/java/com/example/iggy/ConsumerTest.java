package com.example.iggy;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerTest {

    @Test
    void decodePayload_base64EncodedString() {
        String original = "{\"id\":1,\"text\":\"hello from Java producer\"}";
        String encoded = Base64.getEncoder().encodeToString(original.getBytes());

        String result = Consumer.decodePayload(encoded);

        assertEquals(original, result);
    }

    @Test
    void decodePayload_byteList() {
        String original = "hello";
        byte[] bytes = original.getBytes();
        List<Number> byteList = Arrays.asList(
                (Number) bytes[0], bytes[1], bytes[2], bytes[3], bytes[4]
        );

        String result = Consumer.decodePayload(byteList);

        assertEquals(original, result);
    }

    @Test
    void decodePayload_nonBase64StringReturnedAsIs() {
        String plain = "not-base64!!@@";

        String result = Consumer.decodePayload(plain);

        assertEquals(plain, result);
    }

    @Test
    void decodePayload_nullReturnsStringNull() {
        String result = Consumer.decodePayload(null);

        assertEquals("null", result);
    }
}
