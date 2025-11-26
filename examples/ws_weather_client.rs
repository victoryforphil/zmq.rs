// WebSocket weather client example
// This example demonstrates subscribing to weather updates via WebSocket
// Make sure to run ws_weather_server first

mod async_helpers;

use std::convert::TryInto;
use std::error::Error;
use zeromq::{Socket, SocketRecv, SubSocket};

#[async_helpers::main]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    
    let mut socket = SubSocket::new();
    
    // Connect to WebSocket endpoint instead of TCP
    socket
        .connect("ws://127.0.0.1:5555")
        .await
        .expect("Failed to connect to WebSocket server");

    // Subscribe to all weather updates (empty filter subscribes to everything)
    socket.subscribe("").await?;
    
    println!("WebSocket Weather client connected to ws://127.0.0.1:5555");
    println!("Waiting for weather updates...\n");

    loop {
        let weather: String = socket.recv().await?.try_into()?;
        
        let parts: Vec<&str> = weather.split_whitespace().collect();
        if parts.len() == 3 {
            let city = parts[0];
            let temp = parts[1];
            let humidity = parts[2];
            
            println!("Weather Update:");
            println!("  City: {}", city);
            println!("  Temperature: {}°C", temp);
            println!("  Humidity: {}%", humidity);
            println!();
        } else {
            println!("Received: {}", weather);
        }
    }
}
