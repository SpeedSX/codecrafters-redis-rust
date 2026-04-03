#![allow(unused_imports)]
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("Accepted connection from {addr}");
                tokio::spawn(handle_connection(stream));
            }
            Err(e) => {
                println!("error accepting connection: {e}");
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream) {
    const BUFFER_LENGTH: usize = 1024;
    let mut buffer = vec![0u8; BUFFER_LENGTH];

    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => break, // connection closed
            Ok(len) => {
                println!("Received: {}", String::from_utf8_lossy(&buffer[..len]));
                if stream.write_all(b"+PONG\r\n").await.is_err() {
                    break;
                }
            }
            Err(e) => {
                println!("read error: {e}");
                break;
            }
        }
    }
}
