use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod parser;
use parser::RedisValue;

mod command;
use command::RedisCommand;

mod storage;
use storage::Storage;

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let storage = Arc::new(Storage::new());

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

async fn handle_connection(mut stream: TcpStream, storage: Arc<Storage>) {
    const BUFFER_LENGTH: usize = 1024;
    let mut buffer = vec![0u8; BUFFER_LENGTH];

    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => break, // connection closed
            Ok(len) => {
                let raw_string = String::from_utf8_lossy(&buffer[..len]);
                println!("Received: {raw_string}");
                process_input(&raw_string, &mut stream, &storage).await;
            }
            Err(e) => {
                log_connection_error("read", &e);
                break;
            }
        }
    }
}

fn check_result(result: Result<(), std::io::Error>) {
    if let Err(e) = result {
        log_connection_error("write", &e);
    }
}

fn log_connection_error(operation: &str, error: &std::io::Error) {
    if is_client_disconnect(error) {
        println!("connection closed by client during {operation}: {error}");
    } else {
        println!("{operation} error: {error}");
    }
}

fn is_client_disconnect(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
    )
}

async fn process_input(input: &str, stream: &mut TcpStream, storage: &Arc<Storage>) {
    if let Ok(cmd) = RedisCommand::parse(input) {
        if let Ok(response) = get_response(cmd, storage).await {
            check_result(write_response(stream, response.to_string()).await);
        } else {
            println!("Failed to write response for command");
            check_result(write_error_response(stream).await);
        }
    } else {
        println!("Failed to parse input");
        check_result(write_error_response(stream).await);
    }
}

async fn get_response(cmd: RedisCommand, storage: &Arc<Storage>) -> Result<RedisValue, ()> {
    match cmd {
        RedisCommand::Ping => Ok(RedisValue::SimpleString("PONG".into())),

        RedisCommand::Echo(args) => Ok(RedisValue::BulkString(args)),

        RedisCommand::Set(key, value, expire) => {
            storage.set(key, value, expire).await;
            Ok(RedisValue::SimpleString("OK".into()))
        }

        RedisCommand::Get(key) => Ok(storage
            .get(&key)
            .await
            .map_or(RedisValue::NullBulkString, RedisValue::BulkString)),

        RedisCommand::RPush(list_key, elements) => {
            let len =
                i64::try_from(storage.add_to_list(list_key, elements).await).map_err(|_| ())?;
            Ok(RedisValue::Integer(len))
        }

        RedisCommand::LRange(list_key, start_index, end_index) => {
            storage
                .get_list_range(&list_key, start_index, end_index)
                .await
                .map(|vec| RedisValue::Array(vec.into_iter().map(RedisValue::BulkString).collect()))
                .ok_or(())
        }
    }
}

async fn write_error_response(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    stream.write_all(b"-ERR\r\n").await
}

async fn write_response<T: AsRef<str>>(
    stream: &mut TcpStream,
    response: T,
) -> Result<(), std::io::Error> {
    stream.write_all(response.as_ref().as_bytes()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_response_echo() {
        let storage = Arc::new(Storage::new());
        let cmd = RedisCommand::Echo("Hello".to_string());
        let response = get_response(cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::BulkString("Hello".into()));
    }

    #[tokio::test]
    async fn test_get_response_set_get() {
        let storage = Arc::new(Storage::new());
        let set_cmd = RedisCommand::Set("key".to_string(), "value".to_string(), None);
        get_response(set_cmd, &storage).await.unwrap();
        let get_cmd = RedisCommand::Get("key".to_string());
        let response = get_response(get_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::BulkString("value".into()));
    }

    #[tokio::test]
    async fn test_get_response_set_expire_get() {
        let storage = Arc::new(Storage::new());
        let set_cmd = RedisCommand::Set("key".to_string(), "value".to_string(), Some(1000));
        get_response(set_cmd, &storage).await.unwrap();
        let get_cmd = RedisCommand::Get("key".to_string());
        let response = get_response(get_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::BulkString("value".into()));
    }

    #[tokio::test]
    async fn test_get_response_get_nonexistent() {
        let storage = Arc::new(Storage::new());
        let get_cmd = RedisCommand::Get("nonexistent".to_string());
        let response = get_response(get_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::NullBulkString);
    }

    #[tokio::test]
    async fn test_get_response_rpush() {
        let storage = Arc::new(Storage::new());

        let rpush_cmd = RedisCommand::RPush("mylist".to_string(), vec!["1".to_string()]);
        let response = get_response(rpush_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::Integer(1));

        let rpush_cmd = RedisCommand::RPush("mylist".to_string(), vec!["2".to_string()]);
        let response = get_response(rpush_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::Integer(2));

        let rpush_cmd =
            RedisCommand::RPush("mylist".to_string(), vec!["3".to_string(), "4".to_string()]);
        let response = get_response(rpush_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::Integer(4));
    }

    #[tokio::test]
    async fn test_get_response_lrange() {
        let storage = Arc::new(Storage::new());
        let rpush_cmd = RedisCommand::RPush(
            "mylist".to_string(),
            vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()],
        );
        get_response(rpush_cmd, &storage).await.unwrap();
        let lrange_cmd = RedisCommand::LRange("mylist".to_string(), 1, 2);
        let response = get_response(lrange_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::Array(vec![RedisValue::BulkString("b".into()), RedisValue::BulkString("c".into())]));
    }
}
