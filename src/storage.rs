use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use tokio::{sync::RwLock, time::Instant};

enum ItemValue {
    String(String),
    List(VecDeque<String>),
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

    pub async fn set(&self, key: String, value: String, expire: Option<i64>) {
        let expire_at = expire.map(|ms| Instant::now() + Duration::from_millis(ms as u64));
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

    pub async fn append(&self, list_key: String, elements: Vec<String>) -> usize {
        let mut data = self.data.write().await;

        let item = data.entry(list_key).or_insert_with(|| Item {
            value: ItemValue::List(VecDeque::new()),
            expire_at: None,
        });

        if let ItemValue::List(list) = &mut item.value {
            list.extend(elements);
            list.len()
        } else {
            // If the key exists but is not a list, we can choose to overwrite it or ignore the command.
            // Here, we choose to overwrite it with a new list containing the element.
            item.value = ItemValue::List(VecDeque::from(elements));
            1
        }
    }

    pub async fn prepend(&self, list_key: String, elements: Vec<String>) -> usize {
        let mut data = self.data.write().await;

        let item = data.entry(list_key).or_insert_with(|| Item {
            value: ItemValue::List(VecDeque::new()),
            expire_at: None,
        });

        if let ItemValue::List(list) = &mut item.value {
            for element in elements {
                list.push_front(element);
            }
            list.len()
        } else {
            // If the key exists but is not a list, we can choose to overwrite it or ignore the command.
            // Here, we choose to overwrite it with a new list containing the element.
            item.value = ItemValue::List(VecDeque::from(elements));
            1
        }
    }

    pub async fn get_list_range(
        &self,
        list_key: &str,
        start_index: i64,
        end_index: i64,
    ) -> Option<Vec<String>> {
        let data = self.data.read().await;
        if let Some(item) = data.get(list_key)
            && let ItemValue::List(list) = &item.value
        {
            let len = list.len() as i64;
            let start = if start_index < 0 {
                len + start_index
            } else {
                start_index
            };
            let end = if end_index < 0 {
                len + end_index
            } else {
                end_index
            };

            if start >= len || end < 0 || start > end {
                return Some(vec![]);
            }

            let start = start.max(0) as usize;
            let end = (end.min(len - 1)) as usize;

            return Some(list.range(start..=end).cloned().collect());
        }
        Some(vec![])
    }

    pub async fn get_list_len(&self, list_key: &str) -> Option<usize> {
        let data = self.data.read().await;
        if let Some(item) = data.get(list_key)
            && let ItemValue::List(list) = &item.value
        {
            return Some(list.len());
        }
        Some(0)
    }

    pub async fn pop_list_front(&self, list_key: &str) -> Option<String> {
        let mut data = self.data.write().await;
        if let Some(item) = data.get_mut(list_key)
            && let ItemValue::List(list) = &mut item.value
        {
            return list.pop_front().or_else(|| {
                // If the list is empty after popping, we can choose to remove the key from storage.
                data.remove(list_key);
                None
            });
        }
        None
    }

    pub async fn pop_list_front_with_timeout(
        &self,
        list_key: &str,
        timeout: i64,
    ) -> Option<String> {
        let future = async {
            loop {
                let mut data = self.data.write().await;
                if let Some(item) = data.get_mut(list_key)
                    && let ItemValue::List(list) = &mut item.value
                {
                    if let Some(value) = list.pop_front() {
                        // If the list is empty after popping, we can choose to remove the key from storage.
                        if list.is_empty() {
                            data.remove(list_key);
                        }

                        return Some(value);
                    }
                }

                drop(data);
                // Keep waiting when the list is empty or missing; BLPOP should unblock on push or timeout.
                tokio::task::yield_now().await;
            }
        };

        if timeout <= 0 {
            return future.await;
        }

        tokio::time::timeout(std::time::Duration::from_millis(timeout as u64), future)
            .await
            .ok()
            .flatten()
    }

    pub async fn pop_list_front_n(&self, list_key: &str, count: i64) -> Option<Vec<String>> {
        let mut data = self.data.write().await;
        if let Some(item) = data.get_mut(list_key)
            && let ItemValue::List(list) = &mut item.value
        {
            let mut result = Vec::new();
            for _ in 0..count {
                if let Some(value) = list.pop_front() {
                    result.push(value);
                } else {
                    break;
                }
            }

            if list.is_empty() {
                data.remove(list_key);
            }

            return Some(result);
        }
        None
    }
}
