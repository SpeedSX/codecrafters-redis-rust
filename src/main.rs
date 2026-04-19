use std::iter::once;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod redis_value;
use redis_value::{RedisParseError, RedisValue};

mod redis_command;
use redis_command::RedisCommand;

mod storage;
use storage::Storage;

mod parsing;

use crate::storage::RedisError;

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
    let mut read_buf = vec![0u8; BUFFER_LENGTH];
    let mut accumulator = String::new();

    loop {
        match stream.read(&mut read_buf).await {
            Ok(0) => break, // connection closed
            Ok(len) => {
                accumulator.push_str(&String::from_utf8_lossy(&read_buf[..len]));
                println!("Received: {accumulator}");

                loop {
                    match RedisValue::parse_with_rest(&accumulator) {
                        Ok((value, rest)) => {
                            let rest = rest.to_string();

                            match RedisCommand::try_from(&value) {
                                Ok(cmd) => process_command(cmd, &mut stream, &storage).await,
                                Err(_) => {
                                    check_result(write_command_error_response(&mut stream).await);
                                }
                            }

                            accumulator = rest;
                        }
                        Err(RedisParseError::Incomplete) => break,
                        Err(RedisParseError::Protocol) => {
                            check_result(write_protocol_error_response(&mut stream).await);
                            return;
                        }
                    }
                }
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

async fn process_command(cmd: RedisCommand, stream: &mut TcpStream, storage: &Arc<Storage>) {
    match get_response(cmd, storage).await {
        Ok(response) => {
            check_result(write_response(stream, response.to_string()).await);
        }
        Err(error) => {
            println!("Failed to process command: {error}");
            check_result(write_error_response(stream, error).await);
        }
    }
}

async fn get_response(cmd: RedisCommand, storage: &Arc<Storage>) -> Result<RedisValue, RedisError> {
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
            let len = i64::try_from(storage.append(list_key, elements).await)
                .map_err(|_| RedisError::GenericError)?;
            Ok(RedisValue::Integer(len))
        }

        RedisCommand::LPush(list_key, elements) => {
            let len = i64::try_from(storage.prepend(list_key, elements).await)
                .map_err(|_| RedisError::GenericError)?;
            Ok(RedisValue::Integer(len))
        }

        RedisCommand::LRange(list_key, start_index, end_index) => storage
            .get_list_range(&list_key, start_index, end_index)
            .await
            .map(|vec| RedisValue::Array(vec.into_iter().map(RedisValue::BulkString).collect()))
            .ok_or(RedisError::GenericError),

        RedisCommand::LLen(list_key) => storage
            .get_list_len(&list_key)
            .await
            .map(|len| RedisValue::Integer(len as i64))
            .ok_or(RedisError::GenericError),

        RedisCommand::LPop(list_key, count) => {
            if let Some(count) = count {
                Ok(storage
                    .pop_list_front_n(&list_key, count)
                    .await
                    .map_or(RedisValue::NullArray, |vec| {
                        RedisValue::Array(vec.into_iter().map(RedisValue::BulkString).collect())
                    }))
            } else {
                Ok(storage
                    .pop_list_front(&list_key)
                    .await
                    .map_or(RedisValue::NullBulkString, RedisValue::BulkString))
            }
        }

        RedisCommand::BLPop(list_key, timeout) => Ok(storage
            .pop_list_front_with_timeout(&list_key, timeout)
            .await
            .map_or(RedisValue::NullArray, |value| {
                RedisValue::Array(vec![
                    RedisValue::BulkString(list_key),
                    RedisValue::BulkString(value),
                ])
            })),

        RedisCommand::Type(key) => Ok(RedisValue::SimpleString(
            storage.get_type(&key).await.into(),
        )),

        RedisCommand::XAdd(key, (id, seq), kv_array) => storage
            .add_to_stream(&key, id, seq, kv_array)
            .await
            .map(|(id, seq)| RedisValue::BulkString(format!("{id}-{seq}"))),

        RedisCommand::XRange(key, start, end) => storage
            .get_stream_range(&key, start, end)
            .await
            .map(|entries| {
                RedisValue::Array(
                    entries
                        .into_iter()
                        .map(|(id, seq, kv_array)| {
                            let entry_vec = vec![
                                RedisValue::BulkString(format!("{id}-{seq}")),
                                RedisValue::Array(
                                    kv_array
                                        .iter()
                                        .flat_map(|tup| [&tup.0, &tup.1])
                                        .map(|s| RedisValue::BulkString(s.clone()))
                                        .collect(),
                                ),
                            ];
                            RedisValue::Array(entry_vec)
                        })
                        .collect(),
                )
            })
            .ok_or(RedisError::GenericError),
    }
}

async fn write_error_response(
    stream: &mut TcpStream,
    error: RedisError,
) -> Result<(), std::io::Error> {
    match error {
        RedisError::GenericError => write_response(stream, "-ERR\r\n").await,
        RedisError::InvalidStreamIDOrder | RedisError::InvalidStreamID => {
            write_response(stream, format!("-ERR {error}\r\n")).await
        }
    }
}

async fn write_command_error_response(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    write_response(stream, "-ERR\r\n").await
}

async fn write_protocol_error_response(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    write_response(stream, "-ERR Protocol error\r\n").await
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

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
            RedisValue::NullArray,
            "Expected BLPop to return NullArray when the list is empty and timeout is reached"
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

    #[tokio::test]
    async fn test_get_response_blpop_wait_for_element() {
        let storage = Arc::new(Storage::new());

        let waiting = tokio::spawn({
            let storage = storage.clone();
            async move {
                let blpop_cmd = RedisCommand::BLPop("mylist".to_string(), 0);
                get_response(blpop_cmd, &storage).await.unwrap()
            }
        });

        let pushing = tokio::spawn({
            let storage = storage.clone();
            async move {
                // Wait for a short moment to ensure the BLPop command is waiting for an element to be added to the list.
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let rpush_cmd = RedisCommand::RPush("mylist".to_string(), vec!["a".to_string()]);
                get_response(rpush_cmd, &storage).await.unwrap();
            }
        });

        let (waiting_result, _) = tokio::join!(waiting, pushing);

        let response = waiting_result.unwrap();

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
    async fn test_get_response_blpop_two_waiters_two_elements() {
        let storage = Arc::new(Storage::new());

        let waiting1 = tokio::spawn({
            let storage = storage.clone();
            async move {
                let blpop_cmd = RedisCommand::BLPop("mylist".to_string(), 1000);
                get_response(blpop_cmd, &storage).await.unwrap()
            }
        });

        let waiting2 = tokio::spawn({
            let storage = storage.clone();
            async move {
                let blpop_cmd = RedisCommand::BLPop("mylist".to_string(), 1000);
                get_response(blpop_cmd, &storage).await.unwrap()
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let rpush_cmd =
            RedisCommand::RPush("mylist".to_string(), vec!["a".to_string(), "b".to_string()]);
        get_response(rpush_cmd, &storage).await.unwrap();

        let (result1, result2) = tokio::join!(waiting1, waiting2);

        let response1 = result1.unwrap();
        let response2 = result2.unwrap();

        let expected_a = RedisValue::Array(vec![
            RedisValue::BulkString("mylist".to_string()),
            RedisValue::BulkString("a".to_string()),
        ]);
        let expected_b = RedisValue::Array(vec![
            RedisValue::BulkString("mylist".to_string()),
            RedisValue::BulkString("b".to_string()),
        ]);

        assert!(
            (response1 == expected_a && response2 == expected_b)
                || (response1 == expected_b && response2 == expected_a),
            "Expected both waiters to receive one pushed element each"
        );
    }

    #[tokio::test]
    async fn test_get_response_type() {
        let storage = Arc::new(Storage::new());
        let set_cmd = RedisCommand::Set("key".to_string(), "value".to_string(), None);
        get_response(set_cmd, &storage).await.unwrap();

        let type_cmd = RedisCommand::Type("key".to_string());
        let response = get_response(type_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::SimpleString("string".into()));
    }

    #[tokio::test]
    async fn test_get_response_xadd_correct_id_order() {
        let storage = Arc::new(Storage::new());
        let xadd_cmd1 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(12345), Some(1)),
            vec![("field1".to_string(), "value1".to_string())],
        );
        get_response(xadd_cmd1, &storage).await.unwrap();

        let xadd_cmd2 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(12346), Some(0)),
            vec![("field2".to_string(), "value2".to_string())],
        );
        let response = get_response(xadd_cmd2, &storage).await.unwrap();
        assert_eq!(response, RedisValue::BulkString("12346-0".to_string()));
    }

    #[tokio::test]
    async fn test_get_response_xadd_correct_id_sequence_order() {
        let storage = Arc::new(Storage::new());
        let xadd_cmd1 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(1), Some(0)),
            vec![("field1".to_string(), "value1".to_string())],
        );
        get_response(xadd_cmd1, &storage).await.unwrap();

        let xadd_cmd2 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(1), Some(1)),
            vec![("field2".to_string(), "value2".to_string())],
        );
        let response = get_response(xadd_cmd2, &storage).await.unwrap();
        assert_eq!(response, RedisValue::BulkString("1-1".to_string()));
    }

    #[tokio::test]
    async fn test_get_response_xadd_auto_sequence() {
        let storage = Arc::new(Storage::new());
        let xadd_cmd1 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(1), Some(0)),
            vec![("field1".to_string(), "value1".to_string())],
        );
        get_response(xadd_cmd1, &storage).await.unwrap();

        let xadd_cmd2 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(1), None),
            vec![("field2".to_string(), "value2".to_string())],
        );
        let response = get_response(xadd_cmd2, &storage).await.unwrap();
        assert_eq!(response, RedisValue::BulkString("1-1".to_string()));

        let xadd_cmd3 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(2), None),
            vec![("field2".to_string(), "value2".to_string())],
        );
        let response = get_response(xadd_cmd3, &storage).await.unwrap();
        assert_eq!(response, RedisValue::BulkString("2-0".to_string()));
    }

    #[tokio::test]
    async fn test_get_response_xadd_zero_id_auto_sequence() {
        let storage = Arc::new(Storage::new());
        let xadd_cmd = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(0), None),
            vec![("field2".to_string(), "value2".to_string())],
        );
        let response = get_response(xadd_cmd, &storage).await.unwrap();
        assert_eq!(response, RedisValue::BulkString("0-1".to_string()));
    }

    #[tokio::test]
    async fn test_get_response_xadd_auto() {
        let storage = Arc::new(Storage::new());
        let xadd_cmd = RedisCommand::XAdd(
            "mystream".to_string(),
            (None, None),
            vec![("field2".to_string(), "value2".to_string())],
        );
        let response = get_response(xadd_cmd, &storage).await.unwrap();
        // first entry should be formatted as unixtime-0, but since we can't guarantee the exact timestamp in the test, we check that it ends with "-0"
        if let RedisValue::BulkString(id) = response {
            assert!(
                id.ends_with("-0"),
                "Expected ID to end with '-0' but got '{id}'"
            );
        } else {
            panic!("Expected BulkString response but got {:?}", response);
        }
    }

    #[tokio::test]
    async fn test_get_response_xadd_error_on_incorrect_id_order() {
        let storage = Arc::new(Storage::new());
        let xadd_cmd1 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(2), Some(0)),
            vec![("field1".to_string(), "value1".to_string())],
        );
        get_response(xadd_cmd1, &storage).await.unwrap();

        let xadd_cmd2 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(1), Some(1)),
            vec![("field2".to_string(), "value2".to_string())],
        );
        let response = get_response(xadd_cmd2, &storage).await;
        assert_eq!(
            response,
            Err(RedisError::InvalidStreamIDOrder),
            "Expected error when trying to add an entry with an ID that is less than '2-0'"
        );
    }

    #[tokio::test]
    async fn test_get_response_xadd_error_on_incorrect_id_sequence_order() {
        let storage = Arc::new(Storage::new());
        let xadd_cmd1 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(2), Some(2)),
            vec![("field1".to_string(), "value1".to_string())],
        );
        get_response(xadd_cmd1, &storage).await.unwrap();

        let xadd_cmd2 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(2), Some(2)),
            vec![("field2".to_string(), "value2".to_string())],
        );
        let response = get_response(xadd_cmd2, &storage).await;
        assert_eq!(
            response,
            Err(RedisError::InvalidStreamIDOrder),
            "Expected error when trying to add an entry with an ID sequence that is less than '2'"
        );
    }

    #[tokio::test]
    async fn test_get_response_xadd_check_minimum_id() {
        let storage = Arc::new(Storage::new());

        // check error is returned when trying to add an entry with an ID that is less than "0-1"
        let xadd_cmd2 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(0), Some(0)),
            vec![("field2".to_string(), "value2".to_string())],
        );
        let response = get_response(xadd_cmd2, &storage).await;
        assert_eq!(
            response,
            Err(RedisError::InvalidStreamID),
            "Expected error when trying to add an entry with an ID that is less than '0-1'"
        );
    }

    #[tokio::test]
    async fn test_get_response_xrange() {
        let storage = Arc::new(Storage::new());
        let xadd_cmd1 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(1), Some(0)),
            vec![("field1".to_string(), "value1".to_string())],
        );
        get_response(xadd_cmd1, &storage).await.unwrap();

        let xadd_cmd2 = RedisCommand::XAdd(
            "mystream".to_string(),
            (Some(2), Some(0)),
            vec![("field2".to_string(), "value2".to_string())],
        );
        get_response(xadd_cmd2, &storage).await.unwrap();

        let xrange_cmd = RedisCommand::XRange("mystream".to_string(), (1, Some(0)), (2, Some(0)));
        let response = get_response(xrange_cmd, &storage).await.unwrap();
        assert_eq!(
            response,
            RedisValue::Array(vec![
                RedisValue::Array(vec![
                    RedisValue::BulkString("1-0".to_string()),
                    RedisValue::Array(vec![
                        RedisValue::BulkString("field1".to_string()),
                        RedisValue::BulkString("value1".to_string())
                    ])
                ]),
                RedisValue::Array(vec![
                    RedisValue::BulkString("2-0".to_string()),
                    RedisValue::Array(vec![
                        RedisValue::BulkString("field2".to_string()),
                        RedisValue::BulkString("value2".to_string())
                    ])
                ])
            ]),
            "Expected XRange to return both entries in the stream within the specified range"
        );
    }

    async fn connect_test_client() -> (tokio::task::JoinHandle<()>, tokio::net::TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let storage = Arc::new(Storage::new());

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream, storage).await;
        });

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        (server, client)
    }

    #[tokio::test]
    async fn test_handle_connection_keeps_processing_after_command_error() {
        let (server, mut client) = connect_test_client().await;

        client
            .write_all(b"*1\r\n$7\r\nUNKNOWN\r\n*1\r\n$4\r\nPING\r\n")
            .await
            .unwrap();

        let mut response = [0u8; 13];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"-ERR\r\n+PONG\r\n");

        drop(client);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_connection_closes_on_protocol_error() {
        let (server, mut client) = connect_test_client().await;

        client.write_all(b"!oops\r\n").await.unwrap();

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"-ERR Protocol error\r\n");

        server.await.unwrap();
    }
}
