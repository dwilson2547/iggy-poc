package com.example.iggy;

import org.apache.avro.generic.GenericRecord;
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

import java.time.Instant;
import java.util.Base64;
import java.util.Map;

/**
 * Iggy producer that serializes messages using Apache Avro.
 *
 * <p>The Avro schema is defined in {@link Schemas} (canonical source:
 * {@code resources/schemas/event.avsc}).  Messages are sent as raw schemaless
 * Avro bytes so that the consumer can decode them with the same schema.
 *
 * <p>Usage: {@code mvn exec:java -Dexec.mainClass=com.example.iggy.AvroProducer}
 */
public class AvroProducer {
    private static final Logger logger = LoggerFactory.getLogger(AvroProducer.class);

    private static final String API_BASE = "http://localhost:3000";
    private static final String STREAM_NAME = "demo-stream";
    private static final String TOPIC_NAME = "avro-topic";
    private static final int PARTITION_ID = 1;
    private static final long SEND_INTERVAL_MS = 1000;

    private static final com.fasterxml.jackson.databind.ObjectMapper objectMapper =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private static String authToken;

    public static void main(String[] args) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            logger.info("Connecting to Iggy server...");
            logger.info("Logging in...");
            authToken = login(httpClient);
            logger.info("Logged in as iggy.");

            initSystem(httpClient);
            produceMessages(httpClient);
        } catch (InterruptedException e) {
            logger.info("Producer interrupted. Shutting down...");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Fatal error in Avro producer", e);
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

    private static void initSystem(CloseableHttpClient httpClient) throws Exception {
        HttpGet getStream = new HttpGet(API_BASE + "/streams/" + STREAM_NAME);
        getStream.setHeader("Authorization", "Bearer " + authToken);
        try (CloseableHttpResponse response = httpClient.execute(getStream)) {
            if (response.getCode() != 200) {
                HttpPost create = new HttpPost(API_BASE + "/streams");
                create.setHeader("Authorization", "Bearer " + authToken);
                create.setEntity(new StringEntity(
                        "{\"name\":\"" + STREAM_NAME + "\"}", ContentType.APPLICATION_JSON));
                try (CloseableHttpResponse r = httpClient.execute(create)) {
                    logger.info("Stream '{}' created.", STREAM_NAME);
                }
            } else {
                logger.info("Stream '{}' already exists.", STREAM_NAME);
            }
        }

        HttpGet getTopic = new HttpGet(
                API_BASE + "/streams/" + STREAM_NAME + "/topics/" + TOPIC_NAME);
        getTopic.setHeader("Authorization", "Bearer " + authToken);
        try (CloseableHttpResponse response = httpClient.execute(getTopic)) {
            if (response.getCode() != 200) {
                HttpPost create = new HttpPost(
                        API_BASE + "/streams/" + STREAM_NAME + "/topics");
                create.setHeader("Authorization", "Bearer " + authToken);
                create.setEntity(new StringEntity(
                        "{\"name\":\"" + TOPIC_NAME + "\",\"partitions_count\":1}",
                        ContentType.APPLICATION_JSON));
                try (CloseableHttpResponse r = httpClient.execute(create)) {
                    logger.info("Topic '{}' created.", TOPIC_NAME);
                }
            } else {
                logger.info("Topic '{}' already exists.", TOPIC_NAME);
            }
        }
    }

    private static void produceMessages(CloseableHttpClient httpClient) throws Exception {
        logger.info(
                "Producing Avro messages to stream='{}' topic='{}' partition={} every {}ms. Press Ctrl+C to stop.",
                STREAM_NAME, TOPIC_NAME, PARTITION_ID, SEND_INTERVAL_MS);

        int messageId = 0;
        while (true) {
            messageId++;
            try {
                String ts = Instant.now().toString();
                byte[] avroBytes = Schemas.avroSerialize(messageId, "hello from Java Avro producer", ts);
                String payloadB64 = Base64.getEncoder().encodeToString(avroBytes);

                byte[] partitionBytes = {
                    (byte) (PARTITION_ID & 0xFF),
                    (byte) ((PARTITION_ID >> 8) & 0xFF),
                    (byte) ((PARTITION_ID >> 16) & 0xFF),
                    (byte) ((PARTITION_ID >> 24) & 0xFF)
                };
                String partitionB64 = Base64.getEncoder().encodeToString(partitionBytes);

                String url = String.format("%s/streams/%s/topics/%s/messages",
                        API_BASE, STREAM_NAME, TOPIC_NAME);
                HttpPost request = new HttpPost(url);
                request.setHeader("Authorization", "Bearer " + authToken);

                String body = objectMapper.writeValueAsString(Map.of(
                        "partitioning", Map.of("kind", "partition_id", "value", partitionB64),
                        "messages", new Object[]{Map.of("payload", payloadB64)}));
                request.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));

                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    if (response.getCode() >= 200 && response.getCode() < 300) {
                        logger.info("Sent Avro message #{}: id={} text='hello from Java Avro producer' ts={}",
                                messageId, messageId, ts);
                    } else {
                        String resp = EntityUtils.toString(response.getEntity());
                        logger.error("Failed to send Avro message #{}: HTTP {} - {}",
                                messageId, response.getCode(), resp);
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to send Avro message #{}", messageId, e);
            }

            Thread.sleep(SEND_INTERVAL_MS);
        }
    }
}
