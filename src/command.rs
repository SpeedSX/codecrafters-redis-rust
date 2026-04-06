use crate::parser::RedisValue;

pub enum RedisCommand {
    Echo(String),
    Ping,
    Get(String),
    Set(String, String, Option<u32>),
    RPush(String, Vec<String>),
}

impl RedisCommand {
    pub fn parse<T>(input: T) -> Result<Self, ()>
    where
        T: AsRef<str>,
    {
        RedisValue::parse(input).and_then(|value| Self::try_from(&value))
    }

    fn parse_echo_command<'a, I>(mut iter: I) -> Result<RedisCommand, ()>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let value = match iter.next() {
            Some(RedisValue::BulkString(s)) => s.clone(),
            _ => return Err(()),
        };
        Ok(RedisCommand::Echo(value))
    }

    fn parse_get_command<'a, I>(mut iter: I) -> Result<RedisCommand, ()>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let key = match iter.next() {
            Some(RedisValue::BulkString(s)) => s.clone(),
            _ => return Err(()),
        };
        Ok(RedisCommand::Get(key))
    }

    fn parse_set_command<'a, I>(mut iter: I) -> Result<RedisCommand, ()>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let key = match iter.next() {
            Some(RedisValue::BulkString(s)) => s.clone(),
            _ => return Err(()),
        };
        let value = match iter.next() {
            Some(RedisValue::BulkString(s)) => s.clone(),
            _ => return Err(()),
        };
        let expire = if let Some(RedisValue::BulkString(expire_option_str)) = iter.next() {
            let mult = match expire_option_str.to_uppercase().as_str() {
                "PX" => 1,
                "EX" => 1000,
                _ => return Err(()),
            };
            if let Some(RedisValue::BulkString(expire_value_str)) = iter.next() {
                let expire_value = expire_value_str.parse::<u32>().map_err(|_| ())?;
                if expire_value == 0 {
                    return Err(());
                }
                Some(expire_value * mult)
            } else {
                return Err(());
            }
        } else {
            None
        };

        Ok(RedisCommand::Set(key, value, expire))
    }

    fn parse_rpush_command<'a, I>(mut iter: I) -> Result<RedisCommand, ()>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let list_key = match iter.next() {
            Some(RedisValue::BulkString(s)) => s.clone(),
            _ => return Err(()),
        };

        // Should we allow non-bulk string values to be added to the list? For simplicity, we will only allow bulk strings.
        let elements = iter
            .filter_map(|value| {
                if let RedisValue::BulkString(s) = value {
                    Some(s.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        if elements.is_empty() {
            return Err(());
        }

        Ok(RedisCommand::RPush(list_key, elements))
    }
}

impl TryFrom<&RedisValue> for RedisCommand {
    type Error = ();

    fn try_from(value: &RedisValue) -> Result<RedisCommand, Self::Error> {
        match value {
            RedisValue::Array(items) => {
                let mut iter = items.iter();

                let cmd_name = match iter.next() {
                    Some(RedisValue::BulkString(s)) => s.to_uppercase(),
                    _ => return Err(()),
                };

                match cmd_name.as_str() {
                    "PING" => Ok(RedisCommand::Ping),

                    "ECHO" => RedisCommand::parse_echo_command(iter),

                    "SET" => RedisCommand::parse_set_command(iter),

                    "GET" => RedisCommand::parse_get_command(iter),

                    "RPUSH" => RedisCommand::parse_rpush_command(iter),

                    _ => Err(()),
                }
            }
            RedisValue::BulkString(_)
            | RedisValue::Integer(_)
            | RedisValue::SimpleString(_)
            | RedisValue::NullBulkString => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_from_ping() {
        let value = RedisValue::Array(vec![RedisValue::BulkString("PING".to_string())]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::Ping => (),
            _ => panic!("Expected Ping command"),
        }
    }

    #[test]
    fn test_try_from_echo() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("ECHO".to_string()),
            RedisValue::BulkString("Hello, World!".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::Echo(arg) => assert_eq!(arg.to_string(), "Hello, World!"),
            _ => panic!("Expected Echo command"),
        }
    }

    #[test]
    fn test_try_from_set() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("SET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
            RedisValue::BulkString("myvalue".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::Set(key, value, expire) => {
                assert_eq!(key, "mykey");
                assert_eq!(value, "myvalue");
                assert!(expire.is_none());
            }
            _ => panic!("Expected Set command"),
        }
    }

    #[test]
    fn test_try_from_set_with_expire_seconds() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("SET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
            RedisValue::BulkString("myvalue".to_string()),
            RedisValue::BulkString("EX".to_string()),
            RedisValue::BulkString("10".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::Set(key, value, expire) => {
                assert_eq!(key, "mykey");
                assert_eq!(value, "myvalue");
                assert_eq!(expire, Some(10000));
            }
            _ => panic!("Expected Set command with expire"),
        }
    }

    #[test]
    fn test_try_from_set_with_expire_milliseconds() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("SET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
            RedisValue::BulkString("myvalue".to_string()),
            RedisValue::BulkString("PX".to_string()),
            RedisValue::BulkString("100".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::Set(key, value, expire) => {
                assert_eq!(key, "mykey");
                assert_eq!(value, "myvalue");
                assert_eq!(expire, Some(100));
            }
            _ => panic!("Expected Set command with expire"),
        }
    }

    #[test]
    fn test_try_from_get() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("GET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::Get(key) => {
                assert_eq!(key, "mykey");
            }
            _ => panic!("Expected Get command"),
        }
    }

    #[test]
    fn test_try_from_rpush() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("RPUSH".to_string()),
            RedisValue::BulkString("mylist".to_string()),
            RedisValue::BulkString("element".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::RPush(list_key, element) => {
                assert_eq!(list_key, "mylist");
                assert_eq!(element, ["element"]);
            }
            _ => panic!("Expected RPUSH command"),
        }
    }

    #[test]
    fn test_try_from_invalid() {
        let value = RedisValue::BulkString("PING".to_string());
        assert!(RedisCommand::try_from(&value).is_err());

        let value = RedisValue::Array(vec![]);
        assert!(RedisCommand::try_from(&value).is_err());

        let value = RedisValue::Array(vec![RedisValue::BulkString("UNKNOWN".to_string())]);
        assert!(RedisCommand::try_from(&value).is_err());

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("SET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
        ]);
        assert!(RedisCommand::try_from(&value).is_err());

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("SET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
            RedisValue::BulkString("myvalue".to_string()),
            RedisValue::BulkString("EX".to_string()),
        ]);
        assert!(RedisCommand::try_from(&value).is_err());

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("SET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
            RedisValue::BulkString("myvalue".to_string()),
            RedisValue::BulkString("EX".to_string()),
            RedisValue::BulkString("-10".to_string()),
        ]);
        assert!(RedisCommand::try_from(&value).is_err());

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("SET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
            RedisValue::BulkString("myvalue".to_string()),
            RedisValue::BulkString("EX".to_string()),
            RedisValue::BulkString("not_a_number".to_string()),
        ]);
        assert!(RedisCommand::try_from(&value).is_err());

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("RPUSH".to_string()),
            RedisValue::BulkString("mylist".to_string()),
        ]);
        assert!(RedisCommand::try_from(&value).is_err());
    }

    #[test]
    fn test_parse() {
        let input = "*1\r\n$4\r\nPING\r\n";
        let cmd = RedisCommand::parse(input).unwrap();
        match cmd {
            RedisCommand::Ping => (),
            _ => panic!("Expected Ping command"),
        }
    }
}
