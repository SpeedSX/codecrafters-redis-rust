use crate::parser::RedisValue;

pub enum RedisCommand {
    Echo(String),
    Ping,
    Get(String),
    Set(String, String),
}

impl TryFrom<&RedisValue> for RedisCommand {
    type Error = ();

    fn try_from(value: &RedisValue) -> Result<RedisCommand, Self::Error> {
        match value {
            RedisValue::Array(items) => {
                if items.is_empty() {
                    return Err(());
                }
                let cmd_name = match &items[0] {
                    RedisValue::BulkString(s) => s.to_uppercase(),
                    _ => return Err(())
                };
                match cmd_name.as_str() {
                    "PING" => Ok(RedisCommand::Ping),
                    "ECHO" => {
                        let value = match items.get(1) {
                            Some(RedisValue::BulkString(s)) => s.clone(),
                            _ => return Err(())
                        };
                        Ok(RedisCommand::Echo(value))
                    }
                    "SET" => {
                        let key = match items.get(1) {
                            Some(RedisValue::BulkString(s)) => s.clone(),
                            _ => return Err(())
                        };
                        let value = match items.get(2) {
                            Some(RedisValue::BulkString(s)) => s.clone(),
                            _ => return Err(())
                        };
                        Ok(RedisCommand::Set(key, value))
                    }
                    "GET" => {
                        let key = match items.get(1) {
                            Some(RedisValue::BulkString(s)) => s.clone(),
                            _ => return Err(())
                        };
                        Ok(RedisCommand::Get(key))
                    }
                    _ => return Err(())
                }
            }
            _ => Err(())
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_try_from_ping() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("PING".to_string())
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::Ping => (),
            _ => panic!("Expected Ping command")
        }
    }

    #[test]
    fn test_try_from_echo() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("ECHO".to_string()),
            RedisValue::BulkString("Hello, World!".to_string())
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::Echo(arg) => assert_eq!(arg.to_string(), "Hello, World!"),
            _ => panic!("Expected Echo command")
        }
    }

    #[test]
    fn test_try_from_set() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("SET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
            RedisValue::BulkString("myvalue".to_string())
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::Set(key, value) => {
                assert_eq!(key, "mykey");
                assert_eq!(value, "myvalue");
            }
            _ => panic!("Expected Set command")
        }
    }

    #[test]
    fn test_try_from_get() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("GET".to_string()),
            RedisValue::BulkString("mykey".to_string())
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::Get(key) => {
                assert_eq!(key, "mykey");
            }
            _ => panic!("Expected Get command")
        }
    }
}