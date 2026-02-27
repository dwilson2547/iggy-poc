use iggy::clients::client::IggyClient;
use iggy::tcp::client::TcpClient;
use iggy::tcp::config::TcpClientConfig;
use iggy::client::{Client, MessageClient, UserClient};
use iggy::consumer::Consumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollMessages, PollingStrategy};
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use tracing::{info, debug, error};
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
    let client = IggyClient::create(
        TcpClient::create(TcpClientConfig {
            server_address: "127.0.0.1:8090".to_string(),
            ..Default::default()
        })?,
        None,
        None,
    )?;
    
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

    loop {
        let poll_messages = PollMessages {
            consumer: Consumer::new(Identifier::from_str("1")?),
            stream_id: Identifier::from_str(STREAM_NAME)?,
            topic_id: Identifier::from_str(TOPIC_NAME)?,
            partition_id: Some(PARTITION_ID),
            strategy: PollingStrategy::next(),
            count: MESSAGES_PER_BATCH,
            auto_commit: true,
        };

        match client.poll_messages(&poll_messages).await {
            Ok(polled) => {
                if polled.messages.is_empty() {
                    debug!("No new messages — waiting...");
                    tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
                    continue;
                }

                for msg in &polled.messages {
                    let payload = String::from_utf8_lossy(&msg.payload);
                    info!("[offset={}] {}", msg.offset, payload);
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
