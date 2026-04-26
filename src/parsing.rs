use crate::{redis_command::RedisCommandError, redis_value::RedisValue};

pub trait Parse<'a>: Iterator<Item = &'a RedisValue> {
    fn require_bulk_string(&mut self) -> Result<String, RedisCommandError> {
        match self.next() {
            Some(RedisValue::BulkString(s)) => Ok(s.clone()),
            _ => Err(RedisCommandError::Invalid),
        }
    }

    fn require_int_arg(&mut self) -> Result<i64, RedisCommandError> {
        match self.next() {
            Some(RedisValue::BulkString(s)) => {
                s.parse::<i64>().map_err(|_| RedisCommandError::Invalid)
            }
            _ => Err(RedisCommandError::Invalid),
        }
    }

    fn require_float_arg(&mut self) -> Result<f64, RedisCommandError> {
        match self.next() {
            Some(RedisValue::BulkString(s)) => {
                s.parse::<f64>().map_err(|_| RedisCommandError::Invalid)
            }
            _ => Err(RedisCommandError::Invalid),
        }
    }

    fn match_bulk_string(&mut self) -> Option<String> {
        match self.next() {
            Some(RedisValue::BulkString(s)) => Some(s.clone()),
            _ => None,
        }
    }

    fn match_int_arg(&mut self) -> Result<Option<i64>, RedisCommandError> {
        match self.next() {
            Some(RedisValue::BulkString(s)) => s
                .parse::<i64>()
                .map(Some)
                .map_err(|_| RedisCommandError::Invalid),
            _ => Ok(None),
        }
    }
}

impl<'a, I> Parse<'a> for I where I: Iterator<Item = &'a RedisValue> {}
