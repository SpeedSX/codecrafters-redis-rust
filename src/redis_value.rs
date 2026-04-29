use std::{borrow::Cow, fmt::Display};

use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum RedisParseError {
    #[error("incomplete RESP frame")]
    Incomplete,
    #[error("invalid RESP frame")]
    Protocol,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RedisValue {
    Array(Vec<RedisValue>),
    SimpleString(Cow<'static, str>),
    BulkString(String),
    NullBulkString,
    NullArray,
    Integer(i64),
}

impl RedisValue {
    pub fn parse<T>(input: T) -> Result<Self, RedisParseError>
    where
        T: AsRef<str>,
    {
        Self::try_from(input.as_ref())
    }

    pub fn parse_with_rest(s: &str) -> Result<(Self, &str), RedisParseError> {
        match s.chars().next().ok_or(RedisParseError::Incomplete)? {
            '*' => {
                let (len_str, mut rest) = Self::read_next_value_str(s)?;
                if len_str == "-1" {
                    return Ok((RedisValue::NullArray, rest));
                }

                let len = len_str
                    .parse::<usize>()
                    .map_err(|_| RedisParseError::Protocol)?;
                let values = (0..len)
                    .map(|_| {
                        let (value, new_rest) = RedisValue::parse_with_rest(rest)?;
                        rest = new_rest;
                        Ok(value)
                    })
                    .collect::<Result<Vec<_>, RedisParseError>>()?;
                Ok((RedisValue::Array(values), rest))
            }
            '$' => {
                let (len_str, after_header) = Self::read_next_value_str(s)?;
                if len_str == "-1" {
                    return Ok((RedisValue::NullBulkString, after_header));
                }

                let len = len_str
                    .parse::<usize>()
                    .map_err(|_| RedisParseError::Protocol)?;
                let value = after_header.get(..len).ok_or(RedisParseError::Incomplete)?;
                let after_value = after_header.get(len..).ok_or(RedisParseError::Incomplete)?;
                let rest = after_value
                    .strip_prefix("\r\n")
                    .ok_or(RedisParseError::Incomplete)?;
                Ok((RedisValue::BulkString(value.to_string()), rest))
            }
            ':' => {
                let (value_str, rest) = Self::read_next_value_str(s)?;
                let value = value_str
                    .parse::<i64>()
                    .map_err(|_| RedisParseError::Protocol)?;
                Ok((RedisValue::Integer(value), rest))
            }
            '+' => {
                let (value_str, rest) = Self::read_next_value_str(s)?;
                Ok((RedisValue::SimpleString(value_str.to_string().into()), rest))
            }
            _ => Err(RedisParseError::Protocol),
        }
    }

    // Reads the next value string (up to the next "\r\n") and returns it along with the remaining string.
    fn read_next_value_str(s: &str) -> Result<(&str, &str), RedisParseError> {
        let next = s.find("\r\n").ok_or(RedisParseError::Incomplete)?;
        Ok((&s[1..next], &s[next + 2..]))
    }
}

impl TryFrom<&str> for RedisValue {
    type Error = RedisParseError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        RedisValue::parse_with_rest(s).map(|(value, _)| value)
    }
}

impl Display for RedisValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisValue::Array(values) => {
                let mut s = format!("*{}\r\n", values.len());
                for value in values {
                    s.push_str(&value.to_string());
                }
                write!(f, "{s}")
            }
            RedisValue::BulkString(value) => {
                write!(f, "${}\r\n{}\r\n", value.len(), value)
            }
            RedisValue::Integer(value) => {
                write!(f, ":{value}\r\n")
            }
            RedisValue::SimpleString(value) => {
                write!(f, "+{value}\r\n")
            }
            RedisValue::NullBulkString => {
                write!(f, "$-1\r\n")
            }
            RedisValue::NullArray => {
                write!(f, "*-1\r\n")
            }
        }
    }
}

impl TryFrom<String> for RedisValue {
    type Error = RedisParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        RedisValue::try_from(value.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_string() {
        let input = "+OK\r\n";
        let value = RedisValue::parse(input).unwrap();
        assert_eq!(value.to_string(), input);
    }

    #[test]
    fn parse_null_bulk_string() {
        let input = "$-1\r\n";
        let value = RedisValue::parse(input).unwrap();
        assert_eq!(value.to_string(), input);
    }

    #[test]
    fn parse_null_array() {
        let input = "*-1\r\n";
        let value = RedisValue::parse(input).unwrap();
        assert_eq!(value.to_string(), input);
    }

    #[test]
    fn parse_bulk_string() {
        let input = "$6\r\nfoobar\r\n";
        let value = RedisValue::parse(input).unwrap();
        assert_eq!(value.to_string(), input);
    }

    #[test]
    fn parse_empty_bulk_string() {
        let input = "$0\r\n\r\n";
        let value = RedisValue::parse(input).unwrap();
        assert_eq!(value.to_string(), input);
    }

    #[test]
    fn parse_array() {
        let input = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let value = RedisValue::parse(input).unwrap();
        assert_eq!(value.to_string(), input);
    }

    #[test]
    fn parse_integer() {
        let input = ":123\r\n";
        let value = RedisValue::parse(input).unwrap();
        assert_eq!(value.to_string(), input);
    }

    #[test]
    fn parse_incomplete_bulk_string() {
        let input = "$5\r\nfoo";
        let error = RedisValue::parse(input).unwrap_err();
        assert_eq!(error, RedisParseError::Incomplete);
    }

    #[test]
    fn parse_invalid_prefix() {
        let input = "!oops\r\n";
        let error = RedisValue::parse(input).unwrap_err();
        assert_eq!(error, RedisParseError::Protocol);
    }
}
