use crate::parser::RedisValue;

pub enum RedisCommand {
    Echo(RedisValue),
    Ping,
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
                        if items.len() != 2 {
                            return Err(());
                        }
                        Ok(RedisCommand::Echo(items[1].clone()))
                    }
                    _ => return Err(())
                }
            }
            _ => Err(())
        }
    }
}