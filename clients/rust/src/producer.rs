use iggy::prelude::*;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use tracing::{error, info};
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
        Some(stream) => {
            info!("Stream '{}' already exists (id={}).", STREAM_NAME, stream.id);
        }
        None => {
            client.create_stream(STREAM_NAME).await?;
            info!("Stream '{}' created.", STREAM_NAME);
        }
    }

    match client.get_topic(&stream_id, &topic_id).await? {
        Some(topic) => {
            info!("Topic '{}' already exists (id={}).", TOPIC_NAME, topic.id);
        }
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
        "Producing to stream='{}' topic='{}' partition={} every {}s. Press Ctrl+C to stop.",
        STREAM_NAME, TOPIC_NAME, PARTITION_ID, SEND_INTERVAL_SECS
    );

    let stream_id = Identifier::from_str(STREAM_NAME)?;
    let topic_id = Identifier::from_str(TOPIC_NAME)?;
    let partitioning = Partitioning::partition_id(PARTITION_ID);

    let mut message_id = 0u64;
    loop {
        message_id += 1;

        let payload = MessagePayload {
            id: message_id,
            text: "hello from Rust producer".to_string(),
            ts: chrono::Utc::now().to_rfc3339(),
        };

        let json_str = serde_json::to_string(&payload)?;
        let mut messages = vec![IggyMessage::builder()
            .payload(json_str.as_bytes().to_vec().into())
            .build()?];

        match client
            .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
            .await
        {
            Ok(_) => info!("Sent message #{}: {}", message_id, json_str),
            Err(e) => error!("Failed to send message #{}: {}", message_id, e),
        }

        tokio::time::sleep(Duration::from_secs(SEND_INTERVAL_SECS)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_payload_serializes_required_fields() {
        let payload = MessagePayload {
            id: 1,
            text: "hello from Rust producer".to_string(),
            ts: "2024-01-01T00:00:00Z".to_string(),
        };

        let json = serde_json::to_string(&payload).unwrap();

        assert!(json.contains("\"id\":1"));
        assert!(json.contains("hello from Rust producer"));
        assert!(json.contains("\"ts\""));
    }

    #[test]
    fn message_payload_deserializes_correctly() {
        let json = r#"{"id":2,"text":"hello from Rust producer","ts":"2024-01-01T00:00:00Z"}"#;

        let payload: MessagePayload = serde_json::from_str(json).unwrap();

        assert_eq!(payload.id, 2);
        assert_eq!(payload.text, "hello from Rust producer");
        assert_eq!(payload.ts, "2024-01-01T00:00:00Z");
    }

    #[test]
    fn message_payload_id_increments() {
        let payloads: Vec<MessagePayload> = (1..=3)
            .map(|i| MessagePayload {
                id: i,
                text: "hello from Rust producer".to_string(),
                ts: "2024-01-01T00:00:00Z".to_string(),
            })
            .collect();

        let ids: Vec<u64> = payloads.iter().map(|p| p.id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }
}
