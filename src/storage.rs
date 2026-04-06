use std::{collections::HashMap, time::Duration};

use tokio::{sync::RwLock, time::Instant};

enum ItemValue {
    String(String),
    List(Vec<String>),
}

struct Item {
    value: ItemValue,
    expire_at: Option<Instant>,
}

pub struct Storage {
    data: RwLock<HashMap<String, Item>>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub async fn set(&self, key: String, value: String, expire: Option<u32>) {
        let expire_at = expire.map(|ms| Instant::now() + Duration::from_millis(ms.into()));
        let item = Item {
            value: ItemValue::String(value),
            expire_at,
        };
        self.data.write().await.insert(key, item);
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let mut data = self.data.write().await;
        if let Some(item) = data.get(key) {
            if let Some(expire_at) = item.expire_at
                && Instant::now() >= expire_at
            {
                data.remove(key);
                return None;
            }

            return match &item.value {
                ItemValue::String(s) => Some(s.clone()),
                ItemValue::List(_) => None,
            };
        }
        None
    }

    pub async fn add_to_list(&self, list_key: String, elements: Vec<String>) -> usize {
        let mut data = self.data.write().await;

        let item = data.entry(list_key).or_insert_with(|| Item {
            value: ItemValue::List(Vec::new()),
            expire_at: None,
        });

        if let ItemValue::List(list) = &mut item.value {
            list.extend(elements);
            list.len()
        } else {
            // If the key exists but is not a list, we can choose to overwrite it or ignore the command.
            // Here, we choose to overwrite it with a new list containing the element.
            item.value = ItemValue::List(elements);
            1
        }
    }
}
