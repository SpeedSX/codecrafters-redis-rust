use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod redis_value;
use redis_value::RedisValue;

mod redis_command;
use redis_command::RedisCommand;

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
            println!("Failed to process command");
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
            let len = i64::try_from(storage.append(list_key, elements).await).map_err(|_| ())?;
            Ok(RedisValue::Integer(len))
        }

        RedisCommand::LPush(list_key, elements) => {
            let len = i64::try_from(storage.prepend(list_key, elements).await).map_err(|_| ())?;
            Ok(RedisValue::Integer(len))
        }

        RedisCommand::LRange(list_key, start_index, end_index) => storage
            .get_list_range(&list_key, start_index, end_index)
            .await
            .map(|vec| RedisValue::Array(vec.into_iter().map(RedisValue::BulkString).collect()))
            .ok_or(()),

        RedisCommand::LLen(list_key) => storage
            .get_list_len(&list_key)
            .await
            .map(|len| RedisValue::Integer(len as i64))
            .ok_or(()),

        RedisCommand::LPop(list_key, count) => {
            if let Some(count) = count {
                Ok(storage
                    .pop_list_front_n(&list_key, count)
                    .await
                    .map_or(RedisValue::NullBulkString, |vec| {
                        RedisValue::Array(vec.into_iter().map(RedisValue::BulkString).collect())
                    }))
            } else {
                Ok(storage
                    .pop_list_front(&list_key)
                    .await
                    .map_or(RedisValue::NullBulkString, RedisValue::BulkString))
            }
        }

        RedisCommand::BLPop(list_key, timeout) =>
        // TODO: block until element is found or timeout is reached.
        // Return NullBulkString if the list is timeout is reached.
        {
            Ok(storage
                .pop_list_front_with_timeout(&list_key, timeout)
                .await
                .map_or(RedisValue::NullBulkString, |value| {
                    RedisValue::Array(vec![
                        RedisValue::BulkString(list_key),
                        RedisValue::BulkString(value),
                    ])
                }))
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
    async fn test_get_response_lpush() {
        let storage = Arc::new(Storage::new());

        let lpush_cmd = RedisCommand::LPush("mylist".to_string(), vec!["1".to_string()]);
        let response = get_response(lpush_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::Integer(1));

        let lpush_cmd = RedisCommand::LPush("mylist".to_string(), vec!["2".to_string()]);
        let response = get_response(lpush_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::Integer(2));

        let lpush_cmd =
            RedisCommand::LPush("mylist".to_string(), vec!["3".to_string(), "4".to_string()]);
        let response = get_response(lpush_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::Integer(4));
    }

    #[tokio::test]
    async fn test_get_response_lrange_after_rpush() {
        let storage = Arc::new(Storage::new());
        let rpush_cmd = RedisCommand::RPush(
            "mylist".to_string(),
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
            ],
        );
        get_response(rpush_cmd, &storage).await.unwrap();
        let lrange_cmd = RedisCommand::LRange("mylist".to_string(), 1, 2);
        let response = get_response(lrange_cmd, &storage).await.unwrap();
        assert_eq!(
            response,
            RedisValue::Array(vec![
                RedisValue::BulkString("b".into()),
                RedisValue::BulkString("c".into())
            ])
        );
    }

    #[tokio::test]
    async fn test_get_response_lrange_after_lpush() {
        let storage = Arc::new(Storage::new());
        let rpush_cmd =
            RedisCommand::RPush("mylist".to_string(), vec!["a".to_string(), "b".to_string()]);
        get_response(rpush_cmd, &storage).await.unwrap();
        let lpush_cmd =
            RedisCommand::LPush("mylist".to_string(), vec!["c".to_string(), "d".to_string()]);
        get_response(lpush_cmd, &storage).await.unwrap();
        let lrange_cmd = RedisCommand::LRange("mylist".to_string(), 1, 2);
        let response = get_response(lrange_cmd, &storage).await.unwrap();
        assert_eq!(
            response,
            RedisValue::Array(vec![
                RedisValue::BulkString("c".into()),
                RedisValue::BulkString("a".into())
            ])
        );
    }

    #[tokio::test]
    async fn test_get_response_lrange_negative_indices() {
        let storage = Arc::new(Storage::new());
        let rpush_cmd = RedisCommand::RPush(
            "mylist".to_string(),
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
            ],
        );
        get_response(rpush_cmd, &storage).await.unwrap();
        let lrange_cmd = RedisCommand::LRange("mylist".to_string(), -3, -1);
        let response = get_response(lrange_cmd, &storage).await.unwrap();
        assert_eq!(
            response,
            RedisValue::Array(vec![
                RedisValue::BulkString("b".into()),
                RedisValue::BulkString("c".into()),
                RedisValue::BulkString("d".into())
            ])
        );
    }

    #[tokio::test]
    async fn test_get_response_llen() {
        let storage = Arc::new(Storage::new());
        let rpush_cmd = RedisCommand::RPush(
            "mylist".to_string(),
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
            ],
        );
        get_response(rpush_cmd, &storage).await.unwrap();
        let llen_cmd = RedisCommand::LLen("mylist".to_string());
        let response = get_response(llen_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::Integer(4));
    }

    #[tokio::test]
    async fn test_get_response_llen_nonexistent() {
        let storage = Arc::new(Storage::new());
        let llen_cmd = RedisCommand::LLen("nonexistent".to_string());
        let response = get_response(llen_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::Integer(0));
    }

    #[tokio::test]
    async fn test_get_response_lpop() {
        let storage = Arc::new(Storage::new());
        let rpush_cmd = RedisCommand::RPush(
            "mylist".to_string(),
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        );
        get_response(rpush_cmd, &storage).await.unwrap();
        let lpop_cmd = RedisCommand::LPop("mylist".to_string(), None);
        let response = get_response(lpop_cmd, &storage).await.unwrap();
        assert_eq!(
            response,
            RedisValue::BulkString("a".to_string()),
            "Expected to pop the first element 'a' from the list"
        );
        let llen_cmd = RedisCommand::LLen("mylist".to_string());
        let response = get_response(llen_cmd, &storage).await.unwrap();
        assert_eq!(
            response,
            RedisValue::Integer(2),
            "Expected the list length to be 2 after popping one element"
        );
    }

    #[tokio::test]
    async fn test_get_response_lpop_empty() {
        let storage = Arc::new(Storage::new());
        let lpop_cmd = RedisCommand::LPop("mylistempty".to_string(), None);
        let response = get_response(lpop_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::NullBulkString);
    }

    #[tokio::test]
    async fn test_get_response_lpop_n() {
        let storage = Arc::new(Storage::new());
        let rpush_cmd = RedisCommand::RPush(
            "mylist".to_string(),
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        );
        get_response(rpush_cmd, &storage).await.unwrap();
        let lpop_cmd = RedisCommand::LPop("mylist".to_string(), Some(2));
        let response = get_response(lpop_cmd, &storage).await.unwrap();
        assert_eq!(
            response,
            RedisValue::Array(vec![
                RedisValue::BulkString("a".to_string()),
                RedisValue::BulkString("b".to_string())
            ]),
            "Expected to pop the first two elements 'a' and 'b' from the list"
        );
        let llen_cmd = RedisCommand::LLen("mylist".to_string());
        let response = get_response(llen_cmd, &storage).await.unwrap();
        assert_eq!(
            response,
            RedisValue::Integer(1),
            "Expected the list length to be 1 after popping two elements"
        );
    }

    #[tokio::test]
    async fn test_get_response_blpop_timeout() {
        let storage = Arc::new(Storage::new());
        let blpop_cmd = RedisCommand::BLPop("mylist".to_string(), 100);
        let response = get_response(blpop_cmd, &storage).await.unwrap();
        assert_eq!(
            response,
            RedisValue::NullBulkString,
            "Expected BLPop to return NullBulkString when the list is empty and timeout is reached"
        );
    }

    #[tokio::test]
    async fn test_get_response_blpop_with_element() {
        let storage = Arc::new(Storage::new());
        let rpush_cmd = RedisCommand::RPush("mylist".to_string(), vec!["a".to_string()]);
        get_response(rpush_cmd, &storage).await.unwrap();
        let blpop_cmd = RedisCommand::BLPop("mylist".to_string(), 100);
        let response = get_response(blpop_cmd, &storage).await.unwrap();
        assert_eq!(
            response,
            RedisValue::Array(vec![
                RedisValue::BulkString("mylist".to_string()),
                RedisValue::BulkString("a".to_string())
            ]),
            "Expected BLPop to return the popped element 'a' along with the list key when the list is not empty"
        );
    }

    #[tokio::test]
    async fn test_get_response_blpop_with_element_zero_timeout() {
        let storage = Arc::new(Storage::new());
        let rpush_cmd = RedisCommand::RPush("mylist".to_string(), vec!["a".to_string()]);
        get_response(rpush_cmd, &storage).await.unwrap();
        let blpop_cmd = RedisCommand::BLPop("mylist".to_string(), 0);
        let response = get_response(blpop_cmd, &storage).await.unwrap();
        assert_eq!(
            response,
            RedisValue::Array(vec![
                RedisValue::BulkString("mylist".to_string()),
                RedisValue::BulkString("a".to_string())
            ]),
            "Expected BLPop to return the popped element 'a' along with the list key when the list is not empty"
        );
    }
}
