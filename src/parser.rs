use std::{borrow::Cow, fmt::Display};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RedisValue {
    Array(Vec<RedisValue>),
    SimpleString(Cow<'static, str>),
    BulkString(String),
    NullBulkString,
    Integer(i64),
}

impl RedisValue {
    pub fn parse<T>(input: T) -> Result<Self, ()>
    where
        T: AsRef<str>,
    {
        Self::try_from(input.as_ref())
    }

    fn parse_with_rest(s: &str) -> Result<(Self, &str), ()> {
        match s.chars().next().ok_or(())? {
            '*' => {
                let next = s.find("\r\n").ok_or(())?;
                let len = s[1..next].parse::<usize>().map_err(|_| ())?;
                let mut rest = &s[next + 2..];
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
                let next = s.find("\r\n").ok_or(())?;
                let len_str = &s[1..next];
                let after_header = &s[next + 2..];
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
                let next = s.find("\r\n").ok_or(())?;
                let value = s[1..next].parse::<i64>().map_err(|_| ())?;
                Ok((RedisValue::Integer(value), &s[next + 2..]))
            }
            '+' => {
                let next = s.find("\r\n").ok_or(())?;
                let value = s[1..next].to_string();
                Ok((RedisValue::SimpleString(value.into()), &s[next + 2..]))
            }
            _ => Err(()),
        }
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
    fn test_simple_string() {
        let input = "+OK\r\n";
        let value = RedisValue::parse(input).unwrap();
        assert_eq!(value.to_string(), input);
    }

    #[test]
    fn test_null_bulk_string() {
        let input = "$-1\r\n";
        let value = RedisValue::parse(input).unwrap();
        assert_eq!(value.to_string(), input);
    }

    #[test]
    fn test_bulk_string() {
        let input = "$6\r\nfoobar\r\n";
        let value = RedisValue::parse(input).unwrap();
        assert_eq!(value.to_string(), input);
    }

    #[test]
    fn test_array() {
        let input = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let value = RedisValue::parse(input).unwrap();
        assert_eq!(value.to_string(), input);
    }

    #[test]
    fn test_integer() {
        let input = ":123\r\n";
        let value = RedisValue::parse(input).unwrap();
        assert_eq!(value.to_string(), input);
    }
}
