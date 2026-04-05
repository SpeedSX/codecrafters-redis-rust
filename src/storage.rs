use std::{collections::HashMap, time::Duration};

use tokio::{sync::RwLock, time::Instant};

struct Item {
    value: String,
    expire_at: Option<Instant>,
}

pub struct Storage {
    data:  RwLock<HashMap<String, Item>>
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub async fn set(&self, key: String, value: String, expire: Option<i32>) {
        let expire_at = expire.map(|ms| Instant::now() + Duration::from_millis(ms as u64));
        let item = Item { value, expire_at };
        self.data.write().await.insert(key, item);
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let mut data = self.data.write().await;
        if let Some(item) = data.get(key) {
            if let Some(expire_at) = item.expire_at {
                if Instant::now() >= expire_at {
                    data.remove(key);
                    return None;
                }
            }
            return Some(item.value.clone());
        }
        None
    }
}