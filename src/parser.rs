use std::fmt::Display;

#[derive(Clone)]
pub enum RedisValue {
    Array(Vec<RedisValue>),
    BulkString(String),
    Integer(i64),
}

impl RedisValue {
    pub fn parse<T>(input: T) -> Result<Self, ()>
    where
        T: AsRef<str>,
    {
        Self::try_from(input.as_ref())
    }

    fn encoded_len(&self) -> usize {
        match self {
            RedisValue::Array(values) => {
                let header_len = 1 + values.len().to_string().len() + 2;
                header_len + values.iter().map(RedisValue::encoded_len).sum::<usize>()
            }
            RedisValue::BulkString(value) => {
                1 + value.len().to_string().len() + 2 + value.len() + 2
            }
            RedisValue::Integer(value) => 1 + value.to_string().len() + 2,
        }
    }
}

impl TryFrom<&str> for RedisValue {
    type Error = ();

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.chars().next().ok_or(())? {
            '*' => {
                let next = s.find('\r').ok_or(())?;
                let len = s[1..next].parse::<usize>().map_err(|_| ())?;
                let mut values = Vec::with_capacity(len);
                let mut rest = &s[next + 2..];
                for _ in 0..len {
                    let value = RedisValue::try_from(rest)?;
                    let consumed = value.encoded_len();
                    values.push(value);
                    rest = &rest[consumed..];
                }
                Ok(RedisValue::Array(values))
            }
            '$' => {
                let next = s.find('\r').ok_or(())?;
                let len = s[1..next].parse::<usize>().map_err(|_| ())?;
                let value = s[next + 2..next + 2 + len].to_string();
                Ok(RedisValue::BulkString(value))
            }
            ':' => {
                let next = s.find('\r').ok_or(())?;
                let value = s[1..next].parse::<i64>().map_err(|_| ())?;
                Ok(RedisValue::Integer(value))
            }
            _ => Err(()),
        }
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
                write!(f, ":{}\r\n", value)
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
