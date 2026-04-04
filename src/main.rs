#![allow(unused_imports)]
use std::os::raw;
use std::os::windows::process;

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod parser;
use parser::RedisValue;

mod command;
use command::RedisCommand;

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
                let raw_string = String::from_utf8_lossy(&buffer[..len]);
                println!("Received: {}", raw_string);
                process_input(&raw_string, &mut stream).await;
            }
            Err(e) => {
                println!("read error: {e}");
                break;
            }
        }
    }
}

fn check_result(result: Result<(), std::io::Error>) {
    if let Err(e) = result {
        println!("write error: {e}");
    }
}

async fn process_input(input: &str, stream: &mut TcpStream) {
    match RedisValue::parse(input) {
        Ok(parsed_value) => {
            if let Ok(cmd) = RedisCommand::try_from(&parsed_value) {
                check_result(process_command(&cmd, stream).await);
            }
        },
        Err(()) => {
            println!("Failed to parse input");
            check_result(stream.write_all(b"-ERR\r\n").await);
        }
    }
}

async fn process_command(cmd: &RedisCommand, stream: &mut TcpStream) -> Result<(), std::io::Error> {
    match cmd {
        RedisCommand::Ping => {
            stream.write_all(b"+PONG\r\n").await
        }
        RedisCommand::Echo(args) => {
            stream.write_all(args.to_string().as_bytes()).await
        }
    }
}