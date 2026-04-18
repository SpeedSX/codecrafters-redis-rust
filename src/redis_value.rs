use std::{borrow::Cow, fmt::Display};

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
    pub fn parse<T>(input: T) -> Result<Self, ()>
    where
        T: AsRef<str>,
    {
        Self::try_from(input.as_ref())
    }

    pub fn parse_with_rest(s: &str) -> Result<(Self, &str), ()> {
        match s.chars().next().ok_or(())? {
            '*' => {
                let (len_str, mut rest) = Self::read_next_value_str(s)?;
                let len = len_str.parse::<usize>().map_err(|_| ())?;
                let values = (0..len)
                    .map(|_| {
                        let (value, new_rest) = RedisValue::parse_with_rest(rest)?;
                        rest = new_rest;
                        Ok(value)
                    })
                    .collect::<Result<Vec<_>, ()>>()?;
                Ok((RedisValue::Array(values), rest))
            }
            '$' => {
                let (len_str, after_header) = Self::read_next_value_str(s)?;
                if len_str == "-1" {
                    return Ok((RedisValue::NullBulkString, after_header));
                }

                let len = len_str.parse::<usize>().map_err(|_| ())?;
                let value = after_header.get(..len).ok_or(())?;
                let after_value = after_header.get(len..).ok_or(())?;
                let rest = after_value.strip_prefix("\r\n").ok_or(())?;
                Ok((RedisValue::BulkString(value.to_string()), rest))
            }
            ':' => {
                let (value_str, rest) = Self::read_next_value_str(s)?;
                let value = value_str.parse::<i64>().map_err(|_| ())?;
                Ok((RedisValue::Integer(value), rest))
            }
            '+' => {
                let (value_str, rest) = Self::read_next_value_str(s)?;
                Ok((RedisValue::SimpleString(value_str.to_string().into()), rest))
            }
            _ => Err(()),
        }
    }

    // Reads the next value string (up to the next "\r\n") and returns it along with the remaining string.
    fn read_next_value_str(s: &str) -> Result<(&str, &str), ()> {
        let next = s.find("\r\n").ok_or(())?;
        Ok((&s[1..next], &s[next + 2..]))
    }
}

impl TryFrom<&str> for RedisValue {
    type Error = ();

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
    type Error = ();

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
}
