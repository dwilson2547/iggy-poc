package com.example.iggy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Optional.empty;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private static final String STREAM_NAME = "demo-stream";
    private static final StreamId STREAM_ID = StreamId.of(STREAM_NAME);

    private static final String TOPIC_NAME = "demo-topic";
    private static final TopicId TOPIC_ID = TopicId.of(TOPIC_NAME);

    private static final long PARTITION_ID = 0L;
    private static final long SEND_INTERVAL_MS = 1000;

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public static void main(String[] args) {
        log.info("Connecting to Iggy server...");
        IggyTcpClient client = IggyTcpClient.builder()
                .host("localhost")
                .port(8090)
                .credentials("iggy", "iggy")
                .build();
        
        // Login
        client.users().login("iggy", "iggy");
        log.info("Connected and logged in as iggy.");

        createStream(client);
        createTopic(client);
        produceMessages(client);
    }

    private static void produceMessages(IggyTcpClient client) {
        log.info(
                "Producing to stream='{}' topic='{}' partition={} every {}ms. Press Ctrl+C to stop.",
                STREAM_NAME,
                TOPIC_NAME,
                PARTITION_ID,
                SEND_INTERVAL_MS);

        int messageId = 0;
        Partitioning partitioning = Partitioning.partitionId(PARTITION_ID);

        while (true) {
            try {
                messageId++;

                // Create JSON payload
                Map<String, Object> payload = new HashMap<>();
                payload.put("id", messageId);
                payload.put("text", "hello from Java SDK producer");
                payload.put("ts", Instant.now().toString());

                String jsonPayload = objectMapper.writeValueAsString(payload);

                // Create message list
                List<Message> messages = new ArrayList<>();
                messages.add(Message.of(jsonPayload));

                // Send messages
                client.messages().sendMessages(STREAM_ID, TOPIC_ID, partitioning, messages);

                log.info("Sent message #{}: {}", messageId, jsonPayload);

                Thread.sleep(SEND_INTERVAL_MS);
            } catch (InterruptedException e) {
                log.info("Producer interrupted. Shutting down...");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Failed to send message #{}", messageId, e);
            }
        }
    }

    private static void createStream(IggyTcpClient client) {
        Optional<StreamDetails> stream = client.streams().getStream(STREAM_ID);
        if (stream.isPresent()) {
            log.info("Stream '{}' already exists (id={}).", STREAM_NAME, stream.get().id());
            return;
        }
        client.streams().createStream(STREAM_NAME);
        log.info("Stream '{}' was created.", STREAM_NAME);
    }

    private static void createTopic(IggyTcpClient client) {
        Optional<TopicDetails> topic = client.topics().getTopic(STREAM_ID, TOPIC_ID);
        if (topic.isPresent()) {
            log.info("Topic '{}' already exists (id={}).", TOPIC_NAME, topic.get().id());
            return;
        }
        client.topics()
                .createTopic(
                        STREAM_ID,
                        1L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        empty(),
                        TOPIC_NAME);
        log.info("Topic '{}' was created.", TOPIC_NAME);
    }
}
