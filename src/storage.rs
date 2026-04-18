use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use thiserror::Error;
use tokio::{
    sync::{Mutex, Notify, RwLock},
    time::Instant,
};

#[derive(Error, Debug, PartialEq, Eq)]
pub enum RedisError {
    #[error("")]
    GenericError,
    #[error("The ID specified in XADD is equal or smaller than the target stream top item")]
    InvalidStreamIDOrder,
    #[error("The ID specified in XADD must be greater than 0-0")]
    InvalidStreamID,
}

type StreamItem = (i64, i64, Vec<(String, String)>);

enum ItemValue {
    String(String),
    List(VecDeque<String>),
    Set(HashSet<String>),
    ZSet(HashMap<String, f64>),
    Stream(VecDeque<StreamItem>),
    VectorSet(Vec<(f64, String)>),
}

struct Item {
    value: ItemValue,
    expire_at: Option<Instant>,
}

pub struct Storage {
    data: RwLock<HashMap<String, Item>>,
    list_append_notify: Mutex<HashMap<String, Arc<Notify>>>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            data: RwLock::new(HashMap::new()),
            list_append_notify: Mutex::new(HashMap::new()),
        }
    }

    async fn get_or_create_list_notify(&self, list_key: &str) -> Arc<Notify> {
        let mut notify_map = self.list_append_notify.lock().await;
        notify_map
            .entry(list_key.to_string())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    pub async fn set(&self, key: String, value: String, expire: Option<i64>) {
        let expire_at = expire.map(|ms| Instant::now() + Duration::from_millis(ms.cast_unsigned()));
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

            // TODO: we can return different types of values based on the actual type of the item, but for now we only support string values.
            return match &item.value {
                ItemValue::String(s) => Some(s.clone()),
                ItemValue::List(_)
                | ItemValue::Set(_)
                | ItemValue::ZSet(_)
                | ItemValue::Stream(_)
                | ItemValue::VectorSet(_) => None,
            };
        }
        None
    }

    pub async fn append(&self, list_key: String, elements: Vec<String>) -> usize {
        let list_notify = self.get_or_create_list_notify(&list_key).await;
        let mut data = self.data.write().await;

        let item = data.entry(list_key.clone()).or_insert_with(|| Item {
            value: ItemValue::List(VecDeque::new()),
            expire_at: None,
        });

        if let ItemValue::List(list) = &mut item.value {
            list.extend(elements);
            list_notify.notify_waiters();
            list.len()
        } else {
            // If the key exists but is not a list, we can choose to overwrite it or ignore the command.
            // Here, we choose to overwrite it with a new list containing the element.
            let len = elements.len();
            item.value = ItemValue::List(VecDeque::from(elements));
            list_notify.notify_waiters();
            len
        }
    }

    pub async fn prepend(&self, list_key: String, elements: Vec<String>) -> usize {
        let list_notify = self.get_or_create_list_notify(&list_key).await;
        let mut data = self.data.write().await;

        let item = data.entry(list_key.clone()).or_insert_with(|| Item {
            value: ItemValue::List(VecDeque::new()),
            expire_at: None,
        });

        if let ItemValue::List(list) = &mut item.value {
            for element in elements {
                list.push_front(element);
            }
            list_notify.notify_waiters();
            list.len()
        } else {
            // If the key exists but is not a list, we can choose to overwrite it or ignore the command.
            // Here, we choose to overwrite it with a new list containing the element.
            let len = elements.len();
            item.value = ItemValue::List(VecDeque::from(elements));
            list_notify.notify_waiters();
            len
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

            let start = usize::try_from(start.max(0)).ok()?;
            let end = usize::try_from(end.min(len - 1)).ok()?;

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
                // Subscribe before checking list state to avoid missing a concurrent wakeup.
                let list_notify = self.get_or_create_list_notify(list_key).await;
                let notified = list_notify.notified();

                if let Some(value) = self.pop_list_front(list_key).await {
                    return Some(value);
                }

                // Keep waiting when the list is empty or missing; BLPOP should unblock on push or timeout.
                notified.await;
            }
        };

        if timeout <= 0 {
            return future.await;
        }

        tokio::time::timeout(
            std::time::Duration::from_millis(timeout.cast_unsigned()),
            future,
        )
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

    pub async fn get_type(&self, key: &str) -> &'static str {
        let data = self.data.read().await;
        if let Some(item) = data.get(key) {
            return match &item.value {
                ItemValue::String(_) => "string",
                ItemValue::List(_) => "list",
                ItemValue::Set(_) => "set",
                ItemValue::ZSet(_) => "zset",
                ItemValue::Stream(_) => "stream",
                ItemValue::VectorSet(_) => "vectorset",
            };
        }
        "none"
    }

    pub async fn add_to_stream(
        &self,
        key: &str,
        id: Option<i64>,
        seq: Option<i64>,
        kv_array: Vec<(String, String)>,
    ) -> Result<(i64, i64), RedisError> {
        if id.unwrap_or(0) < 0 || seq.unwrap_or(0) < 0 || (id.unwrap_or(0) == 0 && seq == Some(0)) {
            return Err(RedisError::InvalidStreamID);
        }

        let mut data = self.data.write().await;

        let storage_item = data.entry(key.to_string()).or_insert_with(|| Item {
            value: ItemValue::Stream(VecDeque::new()),
            expire_at: None,
        });

        let actual_id = id.unwrap_or(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        ); // unix time in milliseconds by default, or the provided id

        if let ItemValue::Stream(stream) = &mut storage_item.value {
            let actual_seq: i64;
            if let Some((last_id, last_seq, _)) = stream.back() {
                // Existing stream
                actual_seq = seq.unwrap_or(if actual_id == *last_id {
                    *last_seq + 1
                } else {
                    0
                }); // auto-increment seq if not provided and id is the same as last_id
                // Check if the new ID is greater than the last ID, or if the ID is the same but the sequence number is greater.
                if actual_id < *last_id || (actual_id == *last_id && actual_seq <= *last_seq) {
                    return Err(RedisError::InvalidStreamIDOrder);
                }
            } else {
                // New stream
                actual_seq = seq.unwrap_or(if actual_id == 0 { 1 } else { 0 });
            }

            stream.push_back((actual_id, actual_seq, kv_array));
            Ok((actual_id, actual_seq))
        } else {
            // If the key exists but is not a stream, we can choose to overwrite it or ignore the command.
            // Here, we choose to overwrite it with a new stream containing the element.
            let actual_seq = seq.unwrap_or(if actual_id == 0 { 1 } else { 0 });
            storage_item.value =
                ItemValue::Stream(VecDeque::from(vec![(actual_id, actual_seq, kv_array)]));
            Ok((actual_id, actual_seq))
        }
    }
}
