use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

#[derive(serde::Deserialize)]
struct User {
    username: String,
}

#[derive(serde::Deserialize)]
struct MessageData {
    author: User,
    content: String,
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let addr: String = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind");
    println!("Listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(accept_connection(stream));
    }

    Ok(())
}

async fn accept_connection(stream: TcpStream) {
    let addr = stream
        .peer_addr()
        .expect("connected streams should have a peer address");
    println!("Peer address: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Error during the websocket handshake occurred");

    println!("New WebSocket connection: {}", addr);

    let (mut write, mut read) = ws_stream.split();

    while let Some(Ok(msg)) = read.next().await {
        match serde_json::from_str::<MessageData>(msg.into_text().unwrap_or("{}".into()).as_str()) {
            Ok(msg) => {
                println!(
                    "Received message from {}: {}",
                    msg.author.username, msg.content
                );
                match write
                    .send(Message::Text(
                        format!("{} sent: {}", msg.author.username, msg.content).into(),
                    ))
                    .await
                {
                    Ok(_) => {
                        println!("Message sent successfully");
                    }
                    Err(e) => {
                        println!("Failed to send message: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("Failed to parse message: {}", e);
            }
        }
    }
    ()
}
