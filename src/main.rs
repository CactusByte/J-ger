use chrono::{Local, NaiveDateTime, TimeZone};
use colored::*;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url; // Import pump functions and structs

#[derive(Debug, Deserialize)]
struct TradeCreatedMessage {
    signature: String,
    sol_amount: u64,
    token_amount: u64,
    is_buy: bool,
    user: String,
    timestamp: u64,
    mint: String,
    virtual_sol_reserves: u64,
    virtual_token_reserves: u64,
    slot: u64,
    tx_index: u64,
    name: String,
    symbol: String,
    description: String,
    image_uri: Option<String>,
    video_uri: Option<String>,
    metadata_uri: String,
    twitter: Option<String>,
    telegram: Option<String>,
    bonding_curve: String,
    associated_bonding_curve: String,
    creator: String,
    created_timestamp: u64,
    raydium_pool: Option<String>,
    complete: bool,
    total_supply: u64,
    website: Option<String>,
    show_name: bool,
    king_of_the_hill_timestamp: Option<u64>,
    market_cap: f64,
    reply_count: u64,
    last_reply: Option<u64>,
    nsfw: bool,
    market_id: Option<String>,
    inverted: Option<bool>,
    is_currently_live: bool,
    creator_username: Option<String>,
    creator_profile_image: Option<String>,
    usd_market_cap: f64,
}

// Function to log received messages
fn handle_incoming_message(message: &str) {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    println!("{}: Received message", timestamp.cyan());
}

// Function to log sent messages
fn handle_outgoing_message(message: &str) {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    println!("{}: Sent message: {}", timestamp.green(), message);
}
#[tokio::main]
async fn main() {
    // WebSocket server URL
    let ws_url = "wss://frontend-api.pump.fun/socket.io/?EIO=4&transport=websocket";

    // Connect to the WebSocket server
    let url = Url::parse(ws_url).expect("Invalid WebSocket URL");
    let (ws_stream, _) = connect_async(url.to_string())
        .await
        .expect("Failed to connect");

    println!("{}", "Connected to WebSocket server.".green());

    // Split the stream into sender and receiver parts
    let (mut write, mut read) = ws_stream.split();

    // Send an initialization message (e.g., "40")
    if let Err(e) = write.send(Message::Text("40".to_string())).await {
        eprintln!("{}", format!("Error sending message: {}", e).red());
    }

    println!("{}", "Sent initialization message.".green());

    // Process incoming messages
    while let Some(msg) = read.next().await {
        match msg {
            Ok(message) => match message {
                Message::Text(text) => {
                    handle_incoming_message(&text);

                    // Respond with "3" if the message is "2"
                    if text.trim() == "2" {
                        let response = "3";
                        if let Err(e) = write.send(Message::Text(response.to_string())).await {
                            eprintln!("Failed to send message: {}", e);
                        } else {
                            handle_outgoing_message(response);
                        }
                    } else {
                        // Process other messages (e.g., tradeCreated)
                        if text.starts_with("42") {
                            let json_str = &text[2..];
                            if let Ok(parsed) =
                                serde_json::from_str::<(String, serde_json::Value)>(json_str)
                            {
                                let (event_type, trade_data) = parsed;

                                if event_type == "tradeCreated" {
                                    if let Ok(trade) =
                                        serde_json::from_value::<TradeCreatedMessage>(trade_data)
                                    {
                                        // println!("Trade created: {:?}", trade);

                                        let timestamp = Local::now()
                                            .format("%Y-%m-%d %H:%M:%S%.3f")
                                            .to_string();

                                        let trade_type = if trade.is_buy {
                                            "BUY".green()
                                        } else {
                                            "SELL".red()
                                        };

                                        println!(
                                            "{} | {} | {} | {:.6} SOL | {} Tokens | {} | {}",
                                            timestamp.cyan(),
                                            trade_type,
                                            trade.symbol.yellow(),
                                            trade.sol_amount as f64 / 1_000_000_000.0,
                                            trade.token_amount.to_string().blue(),
                                            trade.signature,
                                            trade.mint.purple()
                                        );
                                    } else {
                                        eprintln!(
                                            "{}",
                                            "Failed to parse tradeCreated message.".red()
                                        );
                                    }
                                } else {
                                    eprintln!(
                                        "{}",
                                        format!("Unsupported event type: {}", event_type).red()
                                    );
                                }
                            } else {
                                eprintln!("{}", format!("Failed to parse message: {}", text).red());
                            }
                        }
                    }
                }
                Message::Binary(_) => {
                    eprintln!(
                        "{}",
                        "Received binary message, which is not supported.".red()
                    );
                }
                _ => {
                    eprintln!("{}", "Received unsupported message type.".red());
                }
            },
            Err(e) => {
                eprintln!("{}", format!("Error reading message: {}", e).red());
            }
        }
    }
}
