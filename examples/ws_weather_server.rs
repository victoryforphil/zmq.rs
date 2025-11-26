// WebSocket weather server example
// This example demonstrates using WebSocket transport with ZeroMQ PUB/SUB pattern
// Run this server and connect clients using ws://localhost:5555

mod async_helpers;

use rand::Rng;
use std::error::Error;
use std::time::Duration;
use zeromq::{Socket, SocketSend};

#[async_helpers::main]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    
    let mut socket = zeromq::PubSocket::new();
    
    // Bind to WebSocket endpoint instead of TCP
    socket
        .bind("ws://127.0.0.1:5555")
        .await
        .expect("Failed to bind WebSocket server");

    println!("WebSocket Weather server started on ws://127.0.0.1:5555");
    println!("Publishing weather updates every second...");

    let mut rng = rand::thread_rng();
    let cities = vec!["NYC", "LAX", "CHI", "SEA", "MIA"];

    loop {
        let city = cities[rng.gen_range(0..cities.len())];
        let temp = rng.gen_range(-20..40);
        let humidity = rng.gen_range(0..100);
        
        let weather_data = format!("{} {} {}", city, temp, humidity);
        println!("Publishing: {}", weather_data);
        
        socket.send(weather_data.into()).await?;
        
        async_helpers::sleep(Duration::from_secs(1)).await;
    }
}
