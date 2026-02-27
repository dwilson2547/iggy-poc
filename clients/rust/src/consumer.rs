use iggy::prelude::*;
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, error, info};
use tracing_subscriber;

const STREAM_NAME: &str = "demo-stream";
const TOPIC_NAME: &str = "demo-topic";
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
        "Consuming from stream='{}' topic='{}' partition={}. Press Ctrl+C to stop.",
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
                    let payload = String::from_utf8_lossy(&msg.payload);
                    info!("[offset={}] {}", msg.header.offset, payload);
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
