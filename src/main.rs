use std::process;
use std::fs::File;
use std::path::Path;
use tokio::sync::mpsc;
use rand::rngs::OsRng;
use std::time::Instant;
use futures::StreamExt;
use std::collections::VecDeque;
use std::io::{self, BufRead, Write};
use serde::{Deserialize, Serialize};
use ed25519_dalek::{Signature, SigningKey, Signer, VerifyingKey, Verifier};

#[derive(Debug, Serialize, Deserialize)]

struct PriceUpdate {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    price: String,
}

struct SignedMessage {
    message: f64,
    signature: Signature,
}

impl SignedMessage {
    fn new(message: f64, signing_key: SigningKey) -> Self {
        let message_bytes: Vec<u8> = bincode::serialize(&message).unwrap();
        let signature = signing_key.sign(&message_bytes);
        SignedMessage { message, signature}
    }

    fn verify(&self, verifying_key: &VerifyingKey) -> bool {
        let message_bytes: Vec<u8> = bincode::serialize(&self.message).unwrap();
        verifying_key.verify(&message_bytes, &self.signature).is_ok()
    }
}

async fn btcusdt_prices(id: usize, duration_secs: u64, tx: mpsc::Sender<SignedMessage>, priv_key: SigningKey) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = "wss://stream.binance.com:9443/ws/btcusdt@trade";

    let (ws_stream, _) = tokio_tungstenite::connect_async(url).await?;
    let (_, mut receiver) = ws_stream.split();

    let mut prices = VecDeque::new();
    let start_time = Instant::now();

    while start_time.elapsed().as_secs() < duration_secs {
        if let Some(msg) = receiver.next().await {
            let msg = msg?;
            if let Ok(text) = msg.to_text() {
                if let Ok(update) = serde_json::from_str::<PriceUpdate>(text) {
                    if update.symbol == "BTCUSDT" {
                        if let Ok(price) = update.price.parse::<f64>() {
                            prices.push_back(price);
                        }
                    }
                }
            }
        }
    }

    let average_price: f64 = prices.iter().sum::<f64>() / prices.len() as f64;
    let output = format!("Client{} complete. The average USD price of BTC is: ${}", id+1, average_price);
    
    let filename = format!("output_{}.txt", id+1);
    let filename2 = format!("data_points_{}.txt", id+1);

    let _ = save_to_file(filename.as_str(), &output);
    let _ = save_to_file(filename2.as_str(), &prices.iter().map(|p| p.to_string()).collect::<Vec<_>>().join("\n"));

    println!("{}", output);

    tx.send(SignedMessage::new(average_price, priv_key)).await?;

    Ok(())
}

async fn cache_mode_client(id: usize, duration_secs: u64, tx: mpsc::Sender<SignedMessage>, priv_key: SigningKey, _pub_keys: Vec<VerifyingKey>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = btcusdt_prices(id, duration_secs, tx, priv_key).await?;

    // all public keys known by cache mode client as _pub_keys here

    Ok(())
}

async fn aggregator(mut rx: mpsc::Receiver<SignedMessage>, num_clients: usize, pub_keys: Vec<VerifyingKey>) {
    let mut received_values = Vec::new();

    for _ in 0..num_clients {
        if let Some(signed_message) = rx.recv().await {
            let client_index = pub_keys
                .iter()
                .position(|key| signed_message.verify(key))
                .unwrap_or_else(|| {
                    panic!("Received a message with an invalid signature.");
                });

            received_values.push((client_index, signed_message.message));
        }
    }

    let mut all_prices = Vec::new();
    for (_, prices) in received_values {
        all_prices.push(prices);
    }

    let average_price: f64 = all_prices.iter().sum::<f64>() / all_prices.len() as f64;
    let output = format!("Aggregator complete. The final average USD price of BTC is: ${}", average_price);
    let _ = save_to_file("output_aggregator.txt", &output);
    let _ = save_to_file("data_points_aggregator.txt", &all_prices.iter().map(|p| p.to_string()).collect::<Vec<_>>().join("\n"));
    println!("{}", output);
}

fn save_to_file(filename: &str, content: &str) -> Result<(), io::Error> {
    let path = Path::new(filename);
    let mut file = File::create(path)?;

    file.write_all(content.as_bytes())?;

    Ok(())
}

fn read_mode() -> Result<(), io::Error> {
    let output_content = read_from_file("output_aggregator.txt")?;
    let data_points_content = read_from_file("data_points_aggregator.txt")?;

    println!("Output: {}", output_content);
    println!("Data Points:\n{}", data_points_content);

    Ok(())
}

fn read_from_file(filename: &str) -> Result<String, io::Error> {
    let path = Path::new(filename);
    let file = File::open(path)?;

    let content = io::BufReader::new(file)
        .lines()
        .collect::<Result<Vec<String>, _>>()?
        .join("\n");

    Ok(content)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let num_clients = 5;

    match args.get(1).map(String::as_str) {
        Some("--mode=cache") => {
            let duration_secs = if args.len() >= 3 {
                let times_arg = &args[2];
                let parts: Vec<&str> = times_arg.split("=").collect();
                if parts.len() == 2 && parts[0] == "--times" {
                    match parts[1].parse::<u64>() {
                        Ok(t) => t,
                        Err(_) => {
                            println!("Invalid value for --times argument");
                            process::exit(1);
                        }
                    }
                } else {
                    println!("Invalid argument format");
                    process::exit(1);
                }
            } else {
                println!("Missing --times argument");
                process::exit(1);
            };

            let mut csprng = OsRng;

            let priv_keys: Vec<SigningKey> = (0..num_clients).map(|_| SigningKey::generate(&mut csprng)).collect();
            let pub_keys: Vec<VerifyingKey> = priv_keys.iter().map(|sk| sk.verifying_key()).collect();

            let (tx, rx) = mpsc::channel::<SignedMessage>(num_clients);

            let clients = (0..num_clients)
                .map(|i| {
                    let tx1 = tx.clone();
                    let pub_k = pub_keys.clone();
                    let priv_k = priv_keys[i].clone();
                    tokio::spawn(cache_mode_client(i, duration_secs, tx1, priv_k, pub_k))
                })
                .collect::<Vec<_>>();

            tokio::spawn(aggregator(rx, num_clients, pub_keys));

            for client in clients {
                let _ = client.await?;
            }
        }
        Some("--mode=read") => {
            read_mode()?;
        }
        _ => {
            println!("Invalid or missing mode argument.\nUsage: ./binary --mode=cache --times=10 OR ./binary --mode=read\nIf using cargo: cargo run -- --mode=cache --times=10 OR cargo run -- --mode=read");
        }
    }

    Ok(())
}
