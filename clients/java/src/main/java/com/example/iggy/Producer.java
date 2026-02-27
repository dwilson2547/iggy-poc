package com.example.iggy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import java.util.HashMap;
import java.util.Map;

public class Producer {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    
    private static final String API_BASE = "http://localhost:3000";
    private static final String STREAM_NAME = "demo-stream";
    private static final String TOPIC_NAME = "demo-topic";
    private static final int PARTITION_ID = 1;
    private static final long SEND_INTERVAL_MS = 1000;
    
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    
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
            logger.error("Fatal error in producer", e);
            System.exit(1);
        }
    }

    private static String login(CloseableHttpClient httpClient) throws Exception {
        HttpPost request = new HttpPost(API_BASE + "/users/login");
        String loginJson = "{\"username\":\"iggy\",\"password\":\"iggy\"}";
        request.setEntity(new StringEntity(loginJson, ContentType.APPLICATION_JSON));
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String responseBody = EntityUtils.toString(response.getEntity());
            Map<String, Object> loginResponse = objectMapper.readValue(responseBody, Map.class);
            Map<String, String> accessToken = (Map<String, String>) loginResponse.get("access_token");
            return accessToken.get("token");
        }
    }

    private static void initSystem(CloseableHttpClient httpClient) throws Exception {
        // Check if stream exists
        HttpGet getStream = new HttpGet(API_BASE + "/streams/" + STREAM_NAME);
        getStream.setHeader("Authorization", "Bearer " + authToken);
        
        try (CloseableHttpResponse response = httpClient.execute(getStream)) {
            if (response.getCode() == 200) {
                logger.info("Stream '{}' already exists.", STREAM_NAME);
            } else {
                // Create stream
                HttpPost createStream = new HttpPost(API_BASE + "/streams");
                createStream.setHeader("Authorization", "Bearer " + authToken);
                String streamJson = String.format("{\"name\":\"%s\"}", STREAM_NAME);
                createStream.setEntity(new StringEntity(streamJson, ContentType.APPLICATION_JSON));
                try (CloseableHttpResponse createResponse = httpClient.execute(createStream)) {
                    logger.info("Stream '{}' created.", STREAM_NAME);
                }
            }
        }

        // Check if topic exists
        HttpGet getTopic = new HttpGet(API_BASE + "/streams/" + STREAM_NAME + "/topics/" + TOPIC_NAME);
        getTopic.setHeader("Authorization", "Bearer " + authToken);
        
        try (CloseableHttpResponse response = httpClient.execute(getTopic)) {
            if (response.getCode() == 200) {
                logger.info("Topic '{}' already exists.", TOPIC_NAME);
            } else {
                // Create topic
                HttpPost createTopic = new HttpPost(API_BASE + "/streams/" + STREAM_NAME + "/topics");
                createTopic.setHeader("Authorization", "Bearer " + authToken);
                String topicJson = String.format("{\"name\":\"%s\",\"partitions_count\":1}", TOPIC_NAME);
                createTopic.setEntity(new StringEntity(topicJson, ContentType.APPLICATION_JSON));
                try (CloseableHttpResponse createResponse = httpClient.execute(createTopic)) {
                    logger.info("Topic '{}' created.", TOPIC_NAME);
                }
            }
        }
    }

    private static void produceMessages(CloseableHttpClient httpClient) throws Exception {
        logger.info(
                "Producing to stream='{}' topic='{}' partition={} every {}ms. Press Ctrl+C to stop.",
                STREAM_NAME, TOPIC_NAME, PARTITION_ID, SEND_INTERVAL_MS
        );

        int messageId = 0;
        while (true) {
            messageId++;
            
            try {
                // Create JSON payload
                Map<String, Object> payload = new HashMap<>();
                payload.put("id", messageId);
                payload.put("text", "hello from Java producer");
                payload.put("ts", Instant.now().toString());
                
                String jsonPayload = objectMapper.writeValueAsString(payload);
                
                // Base64 encode the payload
                String payloadB64 = Base64.getEncoder().encodeToString(jsonPayload.getBytes());
                
                // Base64 encode the partition ID (4 bytes, little-endian)
                byte[] partitionBytes = new byte[4];
                partitionBytes[0] = (byte) (PARTITION_ID & 0xFF);
                partitionBytes[1] = (byte) ((PARTITION_ID >> 8) & 0xFF);
                partitionBytes[2] = (byte) ((PARTITION_ID >> 16) & 0xFF);
                partitionBytes[3] = (byte) ((PARTITION_ID >> 24) & 0xFF);
                String partitionB64 = Base64.getEncoder().encodeToString(partitionBytes);
                
                // Send message via HTTP
                String url = String.format("%s/streams/%s/topics/%s/messages", API_BASE, STREAM_NAME, TOPIC_NAME);
                HttpPost request = new HttpPost(url);
                request.setHeader("Authorization", "Bearer " + authToken);
                
                // Create message envelope
                Map<String, Object> envelope = new HashMap<>();
                envelope.put("partitioning", Map.of("kind", "partition_id", "value", partitionB64));
                envelope.put("messages", new Object[]{
                    Map.of("payload", payloadB64)
                });
                
                String requestBody = objectMapper.writeValueAsString(envelope);
                request.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_JSON));
                
                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    if (response.getCode() >= 200 && response.getCode() < 300) {
                        logger.info("Sent message #{}: {}", messageId, jsonPayload);
                    } else {
                        String responseBody = EntityUtils.toString(response.getEntity());
                        logger.error("Failed to send message #{}: HTTP {} - {}", messageId, response.getCode(), responseBody);
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to send message #{}", messageId, e);
            }
            
            Thread.sleep(SEND_INTERVAL_MS);
        }
    }
}
