package com.example.iggy;

import com.google.protobuf.DynamicMessage;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Optional;

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
    private static final Logger log = LoggerFactory.getLogger(ProtobufConsumer.class);

    private static final StreamId STREAM_ID = StreamId.of("demo-stream");
    private static final TopicId TOPIC_ID = TopicId.of("protobuf-topic");

    private static final long PARTITION_ID = 0L;
    private static final long MESSAGES_PER_BATCH = 10L;
    private static final long POLL_INTERVAL_MS = 500;

    public static void main(String[] args) {
        log.info("Connecting to Iggy server...");
        IggyTcpClient client = IggyTcpClient.builder()
                .host("localhost")
                .port(8090)
                .credentials("iggy", "iggy")
                .build();

        client.users().login("iggy", "iggy");
        log.info("Connected and logged in as iggy.");

        consumeMessages(client);
    }

    private static void consumeMessages(IggyTcpClient client) {
        log.info(
                "Consuming Protobuf messages from stream='{}' topic='{}' partition={}. Press Ctrl+C to stop.",
                STREAM_ID, TOPIC_ID, PARTITION_ID);

        BigInteger offset = BigInteger.ZERO;
        org.apache.iggy.consumergroup.Consumer consumer =
                org.apache.iggy.consumergroup.Consumer.of(PARTITION_ID);

        while (true) {
            try {
                PolledMessages polledMessages = client.messages().pollMessages(
                        STREAM_ID, TOPIC_ID,
                        Optional.of(PARTITION_ID), consumer,
                        PollingStrategy.offset(offset),
                        MESSAGES_PER_BATCH, true);

                if (polledMessages.messages().isEmpty()) {
                    log.debug("No new messages — waiting...");
                    Thread.sleep(POLL_INTERVAL_MS);
                    continue;
                }

                for (Message message : polledMessages.messages()) {
                    handleMessage(message, offset);
                }

                offset = offset.add(BigInteger.valueOf(polledMessages.messages().size()));
                Thread.sleep(POLL_INTERVAL_MS);

            } catch (InterruptedException e) {
                log.info("Consumer interrupted. Shutting down...");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error while consuming — retrying...", e);
                try {
                    Thread.sleep(POLL_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    static void handleMessage(Message message, BigInteger offset) {
        try {
            DynamicMessage event = Schemas.protobufDeserialize(message.payload());
            log.info("[offset={}] id={} text='{}' ts='{}'",
                    offset,
                    event.getField(Schemas.eventDescriptor().findFieldByName("id")),
                    event.getField(Schemas.eventDescriptor().findFieldByName("text")),
                    event.getField(Schemas.eventDescriptor().findFieldByName("ts")));
        } catch (Exception e) {
            log.error("Failed to decode Protobuf message at offset {}: {}", offset, e.getMessage());
        }
    }
}
