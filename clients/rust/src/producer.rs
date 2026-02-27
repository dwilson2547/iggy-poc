use iggy::clients::client::IggyClient;
use iggy::tcp::client::TcpClient;
use iggy::tcp::config::TcpClientConfig;
use iggy::client::{Client, StreamClient, TopicClient, MessageClient, UserClient};
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use tracing::{info, error};
use tracing_subscriber;

const STREAM_NAME: &str = "demo-stream";
const TOPIC_NAME: &str = "demo-topic";
const PARTITION_ID: u32 = 1;
const SEND_INTERVAL_SECS: u64 = 1;

#[derive(Serialize, Deserialize, Debug)]
struct MessagePayload {
    id: u64,
    text: String,
    ts: String,
}

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

    init_system(&client).await?;
    produce_messages(&client).await?;

    Ok(())
}

async fn init_system(client: &IggyClient) -> Result<(), Box<dyn Error>> {
    // Create stream if it doesn't exist
    match client.get_stream(&Identifier::from_str(STREAM_NAME)?).await {
        Ok(stream) => {
            info!("Stream '{}' already exists (id={}).", STREAM_NAME, stream.id);
        }
        Err(_) => {
            client.create_stream(STREAM_NAME, None).await?;
            info!("Stream '{}' created.", STREAM_NAME);
        }
    }

    // Create topic if it doesn't exist
    match client.get_topic(&Identifier::from_str(STREAM_NAME)?, &Identifier::from_str(TOPIC_NAME)?).await {
        Ok(topic) => {
            info!("Topic '{}' already exists (id={}).", TOPIC_NAME, topic.id);
        }
        Err(_) => {
            client.create_topic(
                &Identifier::from_str(STREAM_NAME)?,
                TOPIC_NAME,
                1,
                Default::default(),
                None,
                None,
                None,
            ).await?;
            info!("Topic '{}' created.", TOPIC_NAME);
        }
    }

    Ok(())
}

async fn produce_messages(client: &IggyClient) -> Result<(), Box<dyn Error>> {
    info!(
        "Producing to stream='{}' topic='{}' partition={} every {}s. Press Ctrl+C to stop.",
        STREAM_NAME, TOPIC_NAME, PARTITION_ID, SEND_INTERVAL_SECS
    );

    let mut message_id = 0u64;
    loop {
        message_id += 1;

        let payload = MessagePayload {
            id: message_id,
            text: "hello from Rust producer".to_string(),
            ts: chrono::Utc::now().to_rfc3339(),
        };

        let json_payload = serde_json::to_string(&payload)?;
        let message = Message::from_str(&json_payload)?;

        let mut send_messages = SendMessages {
            stream_id: Identifier::from_str(STREAM_NAME)?,
            topic_id: Identifier::from_str(TOPIC_NAME)?,
            partitioning: Partitioning::partition_id(PARTITION_ID),
            messages: vec![message],
        };
        
        match client.send_messages(&mut send_messages).await {
            Ok(_) => info!("Sent message #{}: {}", message_id, json_payload),
            Err(e) => error!("Failed to send message #{}: {}", message_id, e),
        }

        tokio::time::sleep(Duration::from_secs(SEND_INTERVAL_SECS)).await;
    }
}
