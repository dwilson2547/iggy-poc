//! Iggy consumer that deserializes Protocol Buffers messages.
//!
//! The Protobuf schema is defined in `schemas.rs` (canonical source:
//! `schemas/event.proto`).  Each message payload is treated as Protobuf
//! wire-format bytes and decoded back to a typed [`schemas::Event`] struct.
//!
//! Usage:
//!   cargo run --bin protobuf_consumer

#[path = "schemas.rs"]
mod schemas;

use iggy::prelude::*;
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, error, info};

const STREAM_NAME: &str = "demo-stream";
const TOPIC_NAME: &str = "protobuf-topic";
const PARTITION_ID: u32 = 1;
const POLL_INTERVAL_MS: u64 = 500;
const MESSAGES_PER_BATCH: u32 = 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    info!("Connecting to Iggy server...");
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address("127.0.0.1:8090".to_string())
        .build()?;

    client.connect().await?;
    info!("Connected. Logging in...");
    client.login_user("iggy", "iggy").await?;
    info!("Logged in as iggy.");

    consume_messages(&client).await?;

    Ok(())
}

async fn consume_messages(client: &IggyClient) -> Result<(), Box<dyn Error>> {
    info!(
        "Consuming Protobuf messages from stream='{}' topic='{}' partition={}. Press Ctrl+C to stop.",
        STREAM_NAME, TOPIC_NAME, PARTITION_ID
    );

    let stream_id = Identifier::from_str(STREAM_NAME)?;
    let topic_id = Identifier::from_str(TOPIC_NAME)?;
    let consumer = Consumer::new(Identifier::from_str("1")?);

    loop {
        match client
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(PARTITION_ID),
                &consumer,
                &PollingStrategy::next(),
                MESSAGES_PER_BATCH,
                true,
            )
            .await
        {
            Ok(polled) => {
                if polled.messages.is_empty() {
                    debug!("No new messages — waiting...");
                    tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
                    continue;
                }

                for msg in &polled.messages {
                    handle_message(msg);
                }

                tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
            }
            Err(e) => {
                error!("Error while consuming — retrying: {}", e);
                tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
            }
        }
    }
}

fn handle_message(msg: &IggyMessage) {
    match schemas::protobuf_deserialize(&msg.payload) {
        Ok(event) => info!(
            "[offset={}] id={} text='{}' ts='{}'",
            msg.header.offset, event.id, event.text, event.ts
        ),
        Err(e) => error!(
            "Failed to decode Protobuf message at offset {}: {}",
            msg.header.offset, e
        ),
    }
}
