//! Iggy producer that serializes messages using Apache Avro.
//!
//! The Avro schema is defined in `schemas.rs` (canonical source:
//! `schemas/event.avsc`).  Messages are sent as raw schemaless Avro bytes so
//! that the consumer can decode them with the same schema.
//!
//! Usage:
//!   cargo run --bin avro_producer

#[path = "schemas.rs"]
mod schemas;

use iggy::prelude::*;
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use tracing::{error, info};

const STREAM_NAME: &str = "demo-stream";
const TOPIC_NAME: &str = "avro-topic";
const PARTITION_ID: u32 = 1;
const SEND_INTERVAL_SECS: u64 = 1;

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

    init_system(&client).await?;
    produce_messages(&client).await?;

    Ok(())
}

async fn init_system(client: &IggyClient) -> Result<(), Box<dyn Error>> {
    let stream_id = Identifier::from_str(STREAM_NAME)?;
    let topic_id = Identifier::from_str(TOPIC_NAME)?;

    match client.get_stream(&stream_id).await? {
        Some(stream) => info!("Stream '{}' already exists (id={}).", STREAM_NAME, stream.id),
        None => {
            client.create_stream(STREAM_NAME).await?;
            info!("Stream '{}' created.", STREAM_NAME);
        }
    }

    match client.get_topic(&stream_id, &topic_id).await? {
        Some(topic) => info!("Topic '{}' already exists (id={}).", TOPIC_NAME, topic.id),
        None => {
            client
                .create_topic(
                    &stream_id,
                    TOPIC_NAME,
                    1,
                    Default::default(),
                    None,
                    IggyExpiry::NeverExpire,
                    MaxTopicSize::ServerDefault,
                )
                .await?;
            info!("Topic '{}' created.", TOPIC_NAME);
        }
    }

    Ok(())
}

async fn produce_messages(client: &IggyClient) -> Result<(), Box<dyn Error>> {
    info!(
        "Producing Avro messages to stream='{}' topic='{}' partition={} every {}s. Press Ctrl+C to stop.",
        STREAM_NAME, TOPIC_NAME, PARTITION_ID, SEND_INTERVAL_SECS
    );

    let stream_id = Identifier::from_str(STREAM_NAME)?;
    let topic_id = Identifier::from_str(TOPIC_NAME)?;
    let partitioning = Partitioning::partition_id(PARTITION_ID);

    let mut message_id: i32 = 0;
    loop {
        message_id += 1;
        let ts = chrono::Utc::now().to_rfc3339();

        match schemas::avro_serialize(message_id, "hello from Rust Avro producer", &ts) {
            Ok(avro_bytes) => {
                let mut messages = vec![IggyMessage::builder()
                    .payload(avro_bytes.into())
                    .build()?];

                match client
                    .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                    .await
                {
                    Ok(_) => info!(
                        "Sent Avro message #{}: id={} text='hello from Rust Avro producer' ts={}",
                        message_id, message_id, ts
                    ),
                    Err(e) => error!("Failed to send Avro message #{}: {}", message_id, e),
                }
            }
            Err(e) => error!("Failed to serialize Avro message #{}: {}", message_id, e),
        }

        tokio::time::sleep(Duration::from_secs(SEND_INTERVAL_SECS)).await;
    }
}
