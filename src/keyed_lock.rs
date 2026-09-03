use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak},
};
use tokio::sync::{Mutex as TokioMutex, OwnedMutexGuard};

/// Manages fine-grained asynchronous mutexes keyed by a string (e.g. `relay_id`).
///
/// Instead of a single process-wide lock, callers lock only the specific key
/// they operate on. Unused locks are automatically collected via weak references.
#[derive(Default)]
pub(crate) struct KeyedLock {
    locks: StdMutex<HashMap<String, Weak<TokioMutex<()>>>>,
}

impl KeyedLock {
    /// Acquire an exclusive asynchronous lock for the given key.
    pub(crate) async fn lock(&self, key: &str) -> OwnedMutexGuard<()> {
        let mutex = {
            let mut map = self.locks.lock().expect("keyed lock poisoned");
            // Periodically evict expired entries to prevent unbounded memory growth
            if map.len() > 1024 {
                map.retain(|_, weak| weak.strong_count() > 0);
            }
            if let Some(weak) = map.get(key) {
                if let Some(arc) = weak.upgrade() {
                    arc
                } else {
                    let arc = Arc::new(TokioMutex::new(()));
                    map.insert(key.to_string(), Arc::downgrade(&arc));
                    arc
                }
            } else {
                let arc = Arc::new(TokioMutex::new(()));
                map.insert(key.to_string(), Arc::downgrade(&arc));
                arc
            }
        };
        mutex.lock_owned().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn independent_keys_do_not_block_each_other() {
        let keyed_lock = Arc::new(KeyedLock::default());

        let lock_a = keyed_lock.lock("key_a").await;

        // "key_b" should be acquired immediately even though "key_a" is held
        let lock_b_res = tokio::time::timeout(
            Duration::from_millis(100),
            keyed_lock.lock("key_b"),
        )
        .await;

        assert!(lock_b_res.is_ok());
        drop(lock_a);
    }

    #[tokio::test]
    async fn same_key_blocks_until_released() {
        let keyed_lock = Arc::new(KeyedLock::default());

        let lock_1 = keyed_lock.lock("same_key").await;

        let lock_2_res = tokio::time::timeout(
            Duration::from_millis(50),
            keyed_lock.lock("same_key"),
        )
        .await;
        assert!(lock_2_res.is_err(), "second lock on same key must block");

        drop(lock_1);

        let lock_2_res = tokio::time::timeout(
            Duration::from_millis(50),
            keyed_lock.lock("same_key"),
        )
        .await;
        assert!(lock_2_res.is_ok(), "lock succeeds after release");
    }
}
