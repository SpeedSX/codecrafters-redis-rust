use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
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

pub type StreamItemId = (i64, i64); // (id, seq)
pub type StreamItem = (StreamItemId, Vec<(String, String)>);
pub enum BoundType {
    Inclusive,
    Exclusive,
}
pub type StreamRangeBound = (i64, Option<i64>, BoundType);

#[allow(dead_code)]
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
    stream_append_notify: Notify,
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            data: RwLock::new(HashMap::new()),
            list_append_notify: Mutex::new(HashMap::new()),
            stream_append_notify: Notify::new(),
        }
    }

    async fn run_with_optional_timeout<T, F>(&self, timeout: u64, future: F) -> Option<T>
    where
        F: Future<Output = T>,
    {
        if timeout == 0 {
            Some(future.await)
        } else {
            tokio::time::timeout(Duration::from_millis(timeout), future)
                .await
                .ok()
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
        timeout: u64,
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

        self.run_with_optional_timeout(timeout, future)
            .await
            .flatten()
    }

    /// Blocks until at least one of the streams has new entries past the exclusive lower bounds,
    /// then returns them. Returns an empty vec on timeout. timeout==0 means wait forever.
    pub async fn read_streams_blocking(
        &self,
        streams: &[(String, (i64, Option<i64>))],
        timeout: u64,
    ) -> Vec<(String, Vec<StreamItem>)> {
        let future = async {
            loop {
                // Subscribe before reading to avoid missing a concurrent append.
                let notified = self.stream_append_notified();
                let raw = self.read_streams_once(streams).await;
                if !raw.is_empty() {
                    return raw;
                }
                notified.await;
            }
        };
        self.run_with_optional_timeout(timeout, future)
            .await
            .unwrap_or_default()
    }

    pub fn stream_append_notified(&self) -> impl Future<Output = ()> + '_ {
        self.stream_append_notify.notified()
    }

    /// Returns entries for each stream key that has new entries after the given exclusive lower bound.
    /// Only streams with at least one matching entry are included. Acquires a single read lock.
    pub async fn read_streams_once(
        &self,
        streams: &[(String, (i64, Option<i64>))],
    ) -> Vec<(String, Vec<StreamItem>)> {
        let data = self.data.read().await;
        streams
            .iter()
            .filter_map(|(stream_key, (start_id, start_seq))| {
                let item = data.get(stream_key)?;
                let ItemValue::Stream(stream) = &item.value else {
                    return None;
                };
                let start: StreamRangeBound = (*start_id, *start_seq, BoundType::Exclusive);
                let end: StreamRangeBound = (i64::MAX, Some(i64::MAX), BoundType::Inclusive);
                let entries: Vec<StreamItem> = stream
                    .iter()
                    .filter(|((id, seq), _)| Self::is_stream_item_in_range(*id, *seq, &start, &end))
                    .cloned()
                    .collect();
                if entries.is_empty() {
                    None
                } else {
                    Some((stream_key.clone(), entries))
                }
            })
            .collect()
    }

    pub async fn get_stream_last_id(&self, key: &str) -> Option<StreamItemId> {
        let data = self.data.read().await;
        if let Some(item) = data.get(key)
            && let ItemValue::Stream(stream) = &item.value
        {
            return stream.back().map(|((id, seq), _)| (*id, *seq));
        }
        None
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
    ) -> Result<StreamItemId, RedisError> {
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
            if let Some(((last_id, last_seq), _)) = stream.back() {
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

            stream.push_back(((actual_id, actual_seq), kv_array));
            self.stream_append_notify.notify_waiters();
            Ok((actual_id, actual_seq))
        } else {
            // If the key exists but is not a stream, we can choose to overwrite it or ignore the command.
            // Here, we choose to overwrite it with a new stream containing the element.
            let actual_seq = seq.unwrap_or(if actual_id == 0 { 1 } else { 0 });
            storage_item.value =
                ItemValue::Stream(VecDeque::from(vec![((actual_id, actual_seq), kv_array)]));
            self.stream_append_notify.notify_waiters();
            Ok((actual_id, actual_seq))
        }
    }

    fn is_stream_item_in_range(
        id: i64,
        seq: i64,
        start_id: &StreamRangeBound,
        end_id: &StreamRangeBound,
    ) -> bool {
        match start_id.2 {
            BoundType::Inclusive => {
                if id < start_id.0 || (id == start_id.0 && seq < start_id.1.unwrap_or(0)) {
                    return false;
                }
            }
            BoundType::Exclusive => {
                if id < start_id.0 || (id == start_id.0 && seq <= start_id.1.unwrap_or(0)) {
                    return false;
                }
            }
        }
        match end_id.2 {
            BoundType::Inclusive => {
                if id > end_id.0 || (id == end_id.0 && seq > end_id.1.unwrap_or(i64::MAX)) {
                    return false;
                }
            }
            BoundType::Exclusive => {
                if id > end_id.0 || (id == end_id.0 && seq >= end_id.1.unwrap_or(i64::MAX)) {
                    return false;
                }
            }
        }
        true
    }

    pub async fn with_stream_range<T, F>(
        &self,
        key: &str,
        start_id: &StreamRangeBound,
        end_id: &StreamRangeBound,
        f: F,
    ) -> Option<T>
    where
        F: for<'a> FnOnce(&mut dyn Iterator<Item = &'a StreamItem>) -> T,
    {
        let data = self.data.read().await;
        if let Some(item) = data.get(key)
            && let ItemValue::Stream(stream) = &item.value
        {
            let mut entries = stream.iter().filter(|((id, seq), _)| {
                Self::is_stream_item_in_range(*id, *seq, start_id, end_id)
            });
            return Some(f(&mut entries));
        }
        None
    }
}
