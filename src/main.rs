use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod parser;
use parser::RedisValue;

mod command;
use command::RedisCommand;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let storage = Arc::new(RwLock::new(HashMap::new()));

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("Accepted connection from {addr}");
                tokio::spawn(handle_connection(stream, storage.clone()));
            }
            Err(e) => {
                println!("error accepting connection: {e}");
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream, storage: Arc<RwLock<HashMap<String, String>>>) {
    const BUFFER_LENGTH: usize = 1024;
    let mut buffer = vec![0u8; BUFFER_LENGTH];

    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => break, // connection closed
            Ok(len) => {
                let raw_string = String::from_utf8_lossy(&buffer[..len]);
                println!("Received: {}", raw_string);
                process_input(&raw_string, &mut stream, &storage).await;
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

async fn process_input(input: &str, stream: &mut TcpStream, storage: &Arc<RwLock<HashMap<String, String>>>) {
    match RedisValue::parse(input) {
        Ok(parsed_value) => {
            if let Ok(cmd) = RedisCommand::try_from(&parsed_value) {
                if let Ok(response) = get_response(&cmd, storage).await {
                    check_result(write_response(stream, response).await);
                } else {
                    println!("Failed to get response for command");
                    check_result(write_error_response(stream).await);
                }
            }
        },
        Err(()) => {
            println!("Failed to parse input");
            check_result(write_error_response(stream).await);
        }
    }
}

async fn get_response(cmd: &RedisCommand, storage: &Arc<RwLock<HashMap<String, String>>>) -> Result<Cow<'static, str>, std::io::Error> {
    match cmd {
        RedisCommand::Ping => {
            return Ok(Cow::Borrowed("+PONG\r\n"));
        }
        RedisCommand::Echo(args) => {
            return Ok(RedisValue::BulkString(args.clone()).to_string().into())
        }
        RedisCommand::Set(key, value) => {
            storage.write().await.insert(key.clone(), value.clone());
            return Ok(Cow::Borrowed("+OK\r\n"));
        }
        RedisCommand::Get(key) => {
            let value = storage.read().await.get(key).cloned();
            match value {
                Some(v) => Ok(RedisValue::BulkString(v).to_string().into()),
                None => Ok(Cow::Borrowed("$-1\r\n"))
            }
        }
    }
}

async fn write_error_response(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    stream.write_all(b"-ERR\r\n").await
}

async fn write_response<T: AsRef<str>>(stream: &mut TcpStream, response: T) -> Result<(), std::io::Error> {
    stream.write_all(response.as_ref().as_bytes()).await
}

mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_response_echo() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let cmd = RedisCommand::Echo("Hello".to_string());
        let response = get_response(&cmd, &storage).await.unwrap();
        assert_eq!(response, "$5\r\nHello\r\n");
    }

    #[tokio::test]
    async fn test_get_response_set_get() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let set_cmd = RedisCommand::Set("key".to_string(), "value".to_string());
        get_response(&set_cmd, &storage).await.unwrap();
        let get_cmd = RedisCommand::Get("key".to_string());
        let response = get_response(&get_cmd, &storage).await.unwrap();
        assert_eq!(response, "$5\r\nvalue\r\n");
    }
}