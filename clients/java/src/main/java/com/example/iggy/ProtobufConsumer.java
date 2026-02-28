package com.example.iggy;

import com.google.protobuf.DynamicMessage;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Iggy consumer that deserializes Protocol Buffers messages.
 *
 * <p>The Protobuf schema is defined in {@link Schemas} (canonical source:
 * {@code resources/schemas/event.proto}).  Each message payload is treated as
 * Protobuf wire-format bytes and decoded back to a {@link DynamicMessage}.
 *
 * <p>Usage: {@code mvn exec:java -Dexec.mainClass=com.example.iggy.ProtobufConsumer}
 */
public class ProtobufConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ProtobufConsumer.class);

    private static final String API_BASE = "http://localhost:3000";
    private static final String STREAM_NAME = "demo-stream";
    private static final String TOPIC_NAME = "protobuf-topic";
    private static final int PARTITION_ID = 1;
    private static final long POLL_INTERVAL_MS = 500;
    private static final int MESSAGES_PER_BATCH = 10;

    private static final com.fasterxml.jackson.databind.ObjectMapper objectMapper =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private static String authToken;
    private static long currentOffset = 0;

    public static void main(String[] args) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            logger.info("Connecting to Iggy server...");
            logger.info("Logging in...");
            authToken = login(httpClient);
            logger.info("Logged in as iggy.");

            consumeMessages(httpClient);
        } catch (InterruptedException e) {
            logger.info("Consumer interrupted. Shutting down...");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Fatal error in Protobuf consumer", e);
            System.exit(1);
        }
    }

    private static String login(CloseableHttpClient httpClient) throws Exception {
        HttpPost request = new HttpPost(API_BASE + "/users/login");
        request.setEntity(new StringEntity(
                "{\"username\":\"iggy\",\"password\":\"iggy\"}", ContentType.APPLICATION_JSON));
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String body = EntityUtils.toString(response.getEntity());
            Map<String, Object> loginResponse = objectMapper.readValue(body, Map.class);
            @SuppressWarnings("unchecked")
            Map<String, String> accessToken = (Map<String, String>) loginResponse.get("access_token");
            return accessToken.get("token");
        }
    }

    private static void consumeMessages(CloseableHttpClient httpClient) throws Exception {
        logger.info(
                "Consuming Protobuf messages from stream='{}' topic='{}' partition={}. Press Ctrl+C to stop.",
                STREAM_NAME, TOPIC_NAME, PARTITION_ID);

        while (true) {
            try {
                String url = String.format(
                        "%s/streams/%s/topics/%s/messages?consumer=1&partition_id=%d&strategy=offset&value=%d&count=%d&auto_commit=true",
                        API_BASE, STREAM_NAME, TOPIC_NAME, PARTITION_ID, currentOffset, MESSAGES_PER_BATCH);

                HttpGet request = new HttpGet(url);
                request.setHeader("Authorization", "Bearer " + authToken);

                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    String responseBody = EntityUtils.toString(response.getEntity());

                    if (response.getCode() == 200 && responseBody != null && !responseBody.trim().isEmpty()) {
                        Map<String, Object> polled = objectMapper.readValue(responseBody, Map.class);
                        @SuppressWarnings("unchecked")
                        List<Map<String, Object>> messages =
                                (List<Map<String, Object>>) polled.get("messages");

                        if (messages == null || messages.isEmpty()) {
                            logger.debug("No new messages — waiting...");
                            Thread.sleep(POLL_INTERVAL_MS);
                            continue;
                        }

                        for (Map<String, Object> msg : messages) {
                            Number offset = (Number) msg.get("offset");
                            handleMessage(offset, msg.get("payload"));
                            currentOffset = offset.longValue() + 1;
                        }
                    } else {
                        logger.debug("No new messages — waiting...");
                    }
                } catch (Exception e) {
                    logger.error("Error while consuming — retrying...", e);
                }

                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                logger.error("Error in poll loop — retrying...", e);
                Thread.sleep(POLL_INTERVAL_MS);
            }
        }
    }

    static void handleMessage(Number offset, Object payloadObj) {
        try {
            byte[] protoBytes = AvroConsumer.decodePayloadBytes(payloadObj);
            DynamicMessage event = Schemas.protobufDeserialize(protoBytes);
            logger.info("[offset={}] id={} text='{}' ts='{}'",
                    offset,
                    event.getField(Schemas.eventDescriptor().findFieldByName("id")),
                    event.getField(Schemas.eventDescriptor().findFieldByName("text")),
                    event.getField(Schemas.eventDescriptor().findFieldByName("ts")));
        } catch (Exception e) {
            logger.error("Failed to decode Protobuf message at offset {}: {}", offset, e.getMessage());
        }
    }
}
