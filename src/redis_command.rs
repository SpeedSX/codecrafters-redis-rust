use std::vec;

use thiserror::Error;

use crate::{
    parsing::Parse,
    redis_value::{RedisParseError, RedisValue},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum RedisCommandError {
    #[error("invalid Redis command")]
    Invalid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum RedisCommandParseError {
    #[error(transparent)]
    Resp(#[from] RedisParseError),
    #[error(transparent)]
    Command(#[from] RedisCommandError),
}

type CommandStreamItemId = (Option<i64>, Option<i64>); // (timestamp, sequence number)
type CommandStreamRangeBound = (i64, Option<i64>);

#[derive(Debug)]
pub enum RedisCommand {
    Echo(String),
    Ping,
    Get(String),
    Set(String, String, Option<i64>),
    RPush(String, Vec<String>),
    LPush(String, Vec<String>),
    LRange(String, i64, i64),
    LLen(String),
    LPop(String, Option<i64>),
    BLPop(String, i64),
    Type(String),
    XAdd(String, CommandStreamItemId, Vec<(String, String)>),
    XRange(String, CommandStreamRangeBound, CommandStreamRangeBound),
    XReadStreams(Vec<(String, CommandStreamRangeBound)>),
}

impl RedisCommand {
    #[allow(dead_code)]
    pub fn parse<T>(input: T) -> Result<Self, RedisCommandParseError>
    where
        T: AsRef<str>,
    {
        let value = RedisValue::parse(input)?;
        Self::try_from(&value).map_err(Into::into)
    }

    #[allow(dead_code)]
    pub fn parse_with_rest(input: &str) -> Result<(Self, &str), RedisCommandParseError> {
        let (value, rest) = RedisValue::parse_with_rest(input)?;
        let cmd = Self::try_from(&value)?;
        Ok((cmd, rest))
    }

    fn parse_echo_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        iter.require_bulk_string().map(RedisCommand::Echo)
    }

    fn parse_get_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        iter.require_bulk_string().map(RedisCommand::Get)
    }

    fn parse_set_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let key = iter.require_bulk_string()?;
        let value = iter.require_bulk_string()?;
        let expire = if let Some(RedisValue::BulkString(expire_option_str)) = iter.next() {
            let mult = match expire_option_str.to_uppercase().as_str() {
                "PX" => 1,
                "EX" => 1000,
                _ => return Err(RedisCommandError::Invalid),
            };
            let expire_value = iter.require_int_arg()?;
            if expire_value > 0 {
                Some(expire_value * mult)
            } else {
                return Err(RedisCommandError::Invalid);
            }
        } else {
            None
        };

        Ok(RedisCommand::Set(key, value, expire))
    }

    fn parse_rpush_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let list_key = iter.require_bulk_string()?;

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
            return Err(RedisCommandError::Invalid);
        }

        Ok(RedisCommand::RPush(list_key, elements))
    }

    fn parse_lpush_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let list_key = iter.require_bulk_string()?;

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
            return Err(RedisCommandError::Invalid);
        }

        Ok(RedisCommand::LPush(list_key, elements))
    }

    fn parse_lrange_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let list_key = iter.require_bulk_string()?;
        let start_index = iter.require_int_arg()?;
        let end_index = iter.require_int_arg()?;
        Ok(RedisCommand::LRange(list_key, start_index, end_index))
    }

    fn parse_llen_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let list_key = iter.require_bulk_string()?;
        Ok(RedisCommand::LLen(list_key))
    }

    fn parse_lpop_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let list_key = iter.require_bulk_string()?;
        let count = iter.match_int_arg()?;

        if let Some(count) = count
            && count <= 0
        {
            return Err(RedisCommandError::Invalid);
        }

        Ok(RedisCommand::LPop(list_key, count))
    }

    fn parse_blpop_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let list_key = iter.require_bulk_string()?;
        let timeout = iter.require_float_arg()?;

        Ok(RedisCommand::BLPop(list_key, (timeout * 1000.0) as i64))
    }

    fn parse_type_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let key = iter.require_bulk_string()?;
        Ok(RedisCommand::Type(key))
    }

    fn parse_xadd_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let key = iter.require_bulk_string()?;
        let id_str = iter.require_bulk_string()?;

        let (id, seq) = if id_str == "*" {
            (None, None) // Use None as a placeholder for auto-generated ID
        } else {
            // Validate the ID format (should be like "12345-0")
            let ids = id_str.split('-').collect::<Vec<&str>>();
            if ids.len() != 2 {
                return Err(RedisCommandError::Invalid);
            }

            let id = ids[0]
                .parse::<i64>()
                .map_err(|_| RedisCommandError::Invalid)?;

            if ids[1] == "*" {
                (Some(id), None)
            } else {
                (
                    Some(id),
                    Some(
                        ids[1]
                            .parse::<i64>()
                            .map_err(|_| RedisCommandError::Invalid)?,
                    ),
                )
            }
        };

        let field = iter.require_bulk_string()?;
        let value = iter.require_bulk_string()?;

        let mut kv_array = vec![(field, value)];

        while let Some(RedisValue::BulkString(field)) = iter.next() {
            let value = iter.require_bulk_string()?;
            kv_array.push((field.clone(), value));
        }

        Ok(RedisCommand::XAdd(key, (id, seq), kv_array))
    }

    fn parse_xrange_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let key = iter.require_bulk_string()?;
        let start_str = iter.require_bulk_string()?;
        let end_str = iter.require_bulk_string()?;

        let start = if start_str == "-" {
            (0, Some(0)) // Special case for "-" which represents the minimum ID
        } else {
            Self::parse_bound(&start_str)?
        };
        let end = if end_str == "+" {
            (i64::MAX, Some(i64::MAX)) // Special case for "+" which represents the maximum ID
        } else {
            Self::parse_bound(&end_str)?
        };

        Ok(RedisCommand::XRange(key, start, end))
    }

    fn parse_xread_command<'a, I>(mut iter: I) -> Result<RedisCommand, RedisCommandError>
    where
        I: Iterator<Item = &'a RedisValue>,
    {
        let source = iter.require_bulk_string()?;
        if source.to_uppercase() == "STREAMS" {
            let mut keys = vec![];
            let mut start_ids = vec![];

            let key = iter.require_bulk_string()?; // require at least one key-ID pair
            keys.push(key);

            while let Some(next) = iter.match_bulk_string() {
                if let Ok(start_id) = Self::parse_bound(&next) {
                    // if a valid start ID is found, it means we've reached the start of the ID list
                    start_ids.push(start_id);
                    while start_ids.len() < keys.len() {
                        if let Some(next) = iter.match_bulk_string() {
                            start_ids.push(Self::parse_bound(&next)?);
                        } else {
                            break;
                        }
                    }
                    break;
                } else {
                    keys.push(next);
                }
            }

            if keys.len() != start_ids.len() {
                return Err(RedisCommandError::Invalid);
            }

            let kv = keys.into_iter().zip(start_ids.into_iter()).collect();
            Ok(RedisCommand::XReadStreams(kv))
        } else {
            Err(RedisCommandError::Invalid)
        }
    }

    fn parse_bound(s: &str) -> Result<(i64, Option<i64>), RedisCommandError> {
        let parts = s.split('-').collect::<Vec<&str>>();
        if parts.len() == 1 {
            let timestamp = parts[0]
                .parse::<i64>()
                .map_err(|_| RedisCommandError::Invalid)?;
            Ok((timestamp, None))
        } else if parts.len() == 2 {
            let timestamp = parts[0]
                .parse::<i64>()
                .map_err(|_| RedisCommandError::Invalid)?;
            let seq = parts[1]
                .parse::<i64>()
                .map_err(|_| RedisCommandError::Invalid)?;
            Ok((timestamp, Some(seq)))
        } else {
            Err(RedisCommandError::Invalid)
        }
    }
}

impl TryFrom<&RedisValue> for RedisCommand {
    type Error = RedisCommandError;

    fn try_from(value: &RedisValue) -> Result<RedisCommand, Self::Error> {
        match value {
            RedisValue::Array(items) => {
                let mut iter = items.iter();

                let cmd_name = match iter.next() {
                    Some(RedisValue::BulkString(s)) => s.to_uppercase(),
                    _ => return Err(RedisCommandError::Invalid),
                };

                match cmd_name.as_str() {
                    "PING" => Ok(RedisCommand::Ping),

                    "ECHO" => RedisCommand::parse_echo_command(iter),

                    "SET" => RedisCommand::parse_set_command(iter),

                    "GET" => RedisCommand::parse_get_command(iter),

                    "RPUSH" => RedisCommand::parse_rpush_command(iter),

                    "LPUSH" => RedisCommand::parse_lpush_command(iter),

                    "LRANGE" => RedisCommand::parse_lrange_command(iter),

                    "LLEN" => RedisCommand::parse_llen_command(iter),

                    "LPOP" => RedisCommand::parse_lpop_command(iter),

                    "BLPOP" => RedisCommand::parse_blpop_command(iter),

                    "TYPE" => RedisCommand::parse_type_command(iter),

                    "XADD" => RedisCommand::parse_xadd_command(iter),

                    "XRANGE" => RedisCommand::parse_xrange_command(iter),

                    "XREAD" => RedisCommand::parse_xread_command(iter),

                    _ => Err(RedisCommandError::Invalid),
                }
            }
            RedisValue::BulkString(_)
            | RedisValue::Integer(_)
            | RedisValue::SimpleString(_)
            | RedisValue::NullBulkString
            | RedisValue::NullArray => Err(RedisCommandError::Invalid),
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
    fn test_try_from_lpush() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("LPUSH".to_string()),
            RedisValue::BulkString("mylist".to_string()),
            RedisValue::BulkString("element".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::LPush(list_key, element) => {
                assert_eq!(list_key, "mylist");
                assert_eq!(element, ["element"]);
            }
            _ => panic!("Expected LPUSH command"),
        }
    }

    #[test]
    fn test_try_from_lrange() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("LRANGE".to_string()),
            RedisValue::BulkString("mylist".to_string()),
            RedisValue::BulkString("0".to_string()),
            RedisValue::BulkString("-1".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::LRange(list_key, start_index, end_index) => {
                assert_eq!(list_key, "mylist");
                assert_eq!(start_index, 0);
                assert_eq!(end_index, -1);
            }
            _ => panic!("Expected LRANGE command"),
        }
    }

    #[test]
    fn test_try_from_llen() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("LLEN".to_string()),
            RedisValue::BulkString("mylist".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::LLen(list_key) => {
                assert_eq!(list_key, "mylist");
            }
            _ => panic!("Expected LLEN command"),
        }
    }

    #[test]
    fn test_try_from_lpop() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("LPOP".to_string()),
            RedisValue::BulkString("mylist".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::LPop(list_key, None) => {
                assert_eq!(list_key, "mylist");
            }
            _ => panic!("Expected LPOP command"),
        }
    }

    #[test]
    fn test_try_from_lpop_n() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("LPOP".to_string()),
            RedisValue::BulkString("mylist".to_string()),
            RedisValue::BulkString("2".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::LPop(list_key, Some(n)) => {
                assert_eq!(list_key, "mylist");
                assert_eq!(n, 2);
            }
            _ => panic!("Expected LPOP 2 command"),
        }
    }

    #[test]
    fn test_try_from_blpop() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("BLPOP".to_string()),
            RedisValue::BulkString("mylist".to_string()),
            RedisValue::BulkString("0.5".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::BLPop(list_key, timeout) => {
                assert_eq!(list_key, "mylist");
                assert_eq!(timeout, 500);
            }
            _ => panic!("Expected BLPOP command"),
        }
    }

    #[test]
    fn test_try_from_type() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("TYPE".to_string()),
            RedisValue::BulkString("mykey".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::Type(key) => {
                assert_eq!(key, "mykey");
            }
            _ => panic!("Expected TYPE command"),
        }
    }

    #[test]
    fn test_try_from_xadd() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("XADD".to_string()),
            RedisValue::BulkString("mystream".to_string()),
            RedisValue::BulkString("12345-1".to_string()),
            RedisValue::BulkString("field1".to_string()),
            RedisValue::BulkString("value1".to_string()),
            RedisValue::BulkString("field2".to_string()),
            RedisValue::BulkString("value2".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::XAdd(key, (id, seq), kv_array) => {
                assert_eq!(key, "mystream");
                assert_eq!(id, Some(12345));
                assert_eq!(seq, Some(1));
                assert_eq!(
                    kv_array,
                    vec![
                        ("field1".to_string(), "value1".to_string()),
                        ("field2".to_string(), "value2".to_string())
                    ]
                );
            }
            _ => panic!("Expected XADD command"),
        }
    }

    #[test]
    fn test_try_from_xadd_auto_generated_seq() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("XADD".to_string()),
            RedisValue::BulkString("mystream".to_string()),
            RedisValue::BulkString("12345-*".to_string()),
            RedisValue::BulkString("field1".to_string()),
            RedisValue::BulkString("value1".to_string()),
            RedisValue::BulkString("field2".to_string()),
            RedisValue::BulkString("value2".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::XAdd(key, (id, seq), kv_array) => {
                assert_eq!(key, "mystream");
                assert_eq!(id, Some(12345));
                assert_eq!(seq, None);
                assert_eq!(
                    kv_array,
                    vec![
                        ("field1".to_string(), "value1".to_string()),
                        ("field2".to_string(), "value2".to_string())
                    ]
                );
            }
            _ => panic!("Expected XADD command"),
        }
    }

    #[test]
    fn test_try_from_xadd_auto_generated_id() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("XADD".to_string()),
            RedisValue::BulkString("mystream".to_string()),
            RedisValue::BulkString("*".to_string()),
            RedisValue::BulkString("field1".to_string()),
            RedisValue::BulkString("value1".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::XAdd(key, (id, seq), kv_array) => {
                assert_eq!(key, "mystream");
                assert_eq!(id, None);
                assert_eq!(seq, None);
                assert_eq!(kv_array, vec![("field1".to_string(), "value1".to_string())]);
            }
            _ => panic!("Expected XADD command"),
        }
    }

    #[test]
    fn test_try_from_xrange_with_seq() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("XRANGE".to_string()),
            RedisValue::BulkString("mystream".to_string()),
            RedisValue::BulkString("12345-1".to_string()),
            RedisValue::BulkString("67890-2".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::XRange(key, start, end) => {
                assert_eq!(key, "mystream");
                assert_eq!(start, (12345, Some(1)));
                assert_eq!(end, (67890, Some(2)));
            }
            _ => panic!("Expected XRANGE command"),
        }
    }

    #[test]
    fn test_try_from_xrange_without_seq() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("XRANGE".to_string()),
            RedisValue::BulkString("mystream".to_string()),
            RedisValue::BulkString("12345".to_string()),
            RedisValue::BulkString("67890".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::XRange(key, start, end) => {
                assert_eq!(key, "mystream");
                assert_eq!(start, (12345, None));
                assert_eq!(end, (67890, None));
            }
            _ => panic!("Expected XRANGE command"),
        }
    }

    #[test]
    fn test_try_from_xrange_from_min() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("XRANGE".to_string()),
            RedisValue::BulkString("mystream".to_string()),
            RedisValue::BulkString("-".to_string()),
            RedisValue::BulkString("67890".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::XRange(key, start, end) => {
                assert_eq!(key, "mystream");
                assert_eq!(start, (0, Some(0)));
                assert_eq!(end, (67890, None));
            }
            _ => panic!("Expected XRANGE command"),
        }
    }

    #[test]
    fn test_try_from_xrange_to_max() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("XRANGE".to_string()),
            RedisValue::BulkString("mystream".to_string()),
            RedisValue::BulkString("12345".to_string()),
            RedisValue::BulkString("+".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::XRange(key, start, end) => {
                assert_eq!(key, "mystream");
                assert_eq!(start, (12345, None));
                assert_eq!(end, (i64::MAX, Some(i64::MAX)));
            }
            _ => panic!("Expected XRANGE command"),
        }
    }

    #[test]
    fn test_try_from_xread_streams() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("XREAD".to_string()),
            RedisValue::BulkString("STREAMS".to_string()),
            RedisValue::BulkString("mystream".to_string()),
            RedisValue::BulkString("12345".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::XReadStreams(kv) => {
                assert_eq!(kv.len(), 1);
                assert_eq!(kv[0].0, "mystream");
                assert_eq!(kv[0].1, (12345, None));
            }
            _ => panic!("Expected XREAD STREAMS command"),
        }
    }

    #[test]
    fn test_try_from_xread_streams_multiple() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString("XREAD".to_string()),
            RedisValue::BulkString("STREAMS".to_string()),
            RedisValue::BulkString("mystream1".to_string()),
            RedisValue::BulkString("mystream2".to_string()),
            RedisValue::BulkString("12345".to_string()),
            RedisValue::BulkString("67890".to_string()),
        ]);
        let cmd = RedisCommand::try_from(&value).unwrap();
        match cmd {
            RedisCommand::XReadStreams(kv) => {
                assert_eq!(kv.len(), 2);
                assert_eq!(kv[0].0, "mystream1");
                assert_eq!(kv[0].1, (12345, None));
                assert_eq!(kv[1].0, "mystream2");
                assert_eq!(kv[1].1, (67890, None));
            }
            _ => panic!("Expected XREAD STREAMS command"),
        }
    }

    #[test]
    fn test_try_from_invalid() {
        let value = RedisValue::BulkString("PING".to_string());
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for non-array value"
        );

        let value = RedisValue::Array(vec![]);
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for empty array"
        );

        let value = RedisValue::Array(vec![RedisValue::BulkString("UNKNOWN".to_string())]);
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for unknown command"
        );

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("SET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
        ]);
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for incomplete SET command"
        );

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("SET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
            RedisValue::BulkString("myvalue".to_string()),
            RedisValue::BulkString("EX".to_string()),
        ]);
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for incomplete SET command with EX option"
        );

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("SET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
            RedisValue::BulkString("myvalue".to_string()),
            RedisValue::BulkString("EX".to_string()),
            RedisValue::BulkString("-10".to_string()),
        ]);
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for negative expiration time"
        );

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("SET".to_string()),
            RedisValue::BulkString("mykey".to_string()),
            RedisValue::BulkString("myvalue".to_string()),
            RedisValue::BulkString("EX".to_string()),
            RedisValue::BulkString("not_a_number".to_string()),
        ]);
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for non-numeric expiration time"
        );

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("RPUSH".to_string()),
            RedisValue::BulkString("mylist".to_string()),
        ]);
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for incomplete RPUSH command"
        );

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("LPUSH".to_string()),
            RedisValue::BulkString("mylist".to_string()),
        ]);
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for incomplete LPUSH command"
        );

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("LPOP".to_string()),
            RedisValue::BulkString("mylist".to_string()),
            RedisValue::BulkString("-2".to_string()),
        ]);
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for negative LPOP count"
        );

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("LPOP".to_string()),
            RedisValue::BulkString("mylist".to_string()),
            RedisValue::BulkString("0".to_string()),
        ]);
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for zero LPOP count"
        );

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("XADD".to_string()),
            RedisValue::BulkString("mystream".to_string()),
            RedisValue::BulkString("12345".to_string()),
            RedisValue::BulkString("field1".to_string()),
            RedisValue::BulkString("value1".to_string()),
        ]);
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for XADD command with invalid ID format"
        );

        let value = RedisValue::Array(vec![
            RedisValue::BulkString("XRANGE".to_string()),
            RedisValue::BulkString("mystream".to_string()),
        ]);
        assert!(
            RedisCommand::try_from(&value).is_err(),
            "Expected error for XRANGE command with missing bounds"
        );
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

    #[test]
    fn test_parse_with_rest_incomplete_frame() {
        let input = "*1\r\n$4\r\nPIN";
        let error = RedisCommand::parse_with_rest(input).unwrap_err();
        assert_eq!(
            error,
            RedisCommandParseError::Resp(RedisParseError::Incomplete)
        );
    }
}
