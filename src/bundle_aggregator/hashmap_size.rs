use std::collections::HashMap;

use payload_archiver::units::Slot;

use super::SlotBundle;

pub trait HasSize {
    fn size_in_bytes(&self) -> usize;
}

pub fn size_of_hashmap<K: HasSize, V: HasSize>(hashmap: &HashMap<K, V>) -> usize {
    hashmap.iter().fold(0, |acc, (key, value)| {
        acc + key.size_in_bytes() + value.size_in_bytes()
    })
}

impl HasSize for Slot {
    fn size_in_bytes(&self) -> usize {
        std::mem::size_of::<i32>()
    }
}

impl HasSize for i32 {
    fn size_in_bytes(&self) -> usize {
        std::mem::size_of::<i32>()
    }
}

impl HasSize for String {
    fn size_in_bytes(&self) -> usize {
        self.len()
    }
}

impl HasSize for i64 {
    fn size_in_bytes(&self) -> usize {
        std::mem::size_of::<i64>()
    }
}

impl HasSize for Vec<u8> {
    fn size_in_bytes(&self) -> usize {
        self.len()
    }
}

impl HasSize for SlotBundle {
    fn size_in_bytes(&self) -> usize {
        self.slot.size_in_bytes()
        // Not accurate, DateTime has more data, but enough for an approximation, this field
        // doesn't contribute much to the size of the bundle.
            + self.earliest.timestamp().size_in_bytes()
            + self.execution_payloads.iter().fold(0, |acc, payload| {
                acc + serde_json::to_vec(payload).unwrap().size_in_bytes()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    #[test]
    fn test_size_of_hashmap() {
        let mut hashmap: HashMap<i32, String> = HashMap::new();
        hashmap.insert(1, "one".to_string());
        hashmap.insert(2, "two".to_string());
        hashmap.insert(3, "three".to_string());

        // Each i32 key is 4 bytes and the length of the strings are 3, 3, and 5 respectively.
        // So, the total size should be 4*3 + 3 + 3 + 5 = 23
        assert_eq!(size_of_hashmap(&hashmap), 23);
    }

    #[test]
    fn test_size_of_hashmap_with_empty_string() {
        let mut hashmap: HashMap<i32, String> = HashMap::new();
        hashmap.insert(1, "".to_string());

        // The key is 4 bytes and the length of the string is 0.
        // So, the total size should be 4 + 0 = 4
        assert_eq!(size_of_hashmap(&hashmap), 4);
    }

    #[test]
    fn test_size_of_hashmap_with_i64_keys() {
        let mut hashmap: HashMap<i64, String> = HashMap::new();
        hashmap.insert(1, "one".to_string());

        // The key is 8 bytes and the length of the string is 3.
        // So, the total size should be 8 + 3 = 11
        assert_eq!(size_of_hashmap(&hashmap), 11);
    }

    #[test]
    fn test_size_of_slot_bundle() {
        let slot_bundle = SlotBundle {
            ackers: vec![],
            earliest: Utc.timestamp_opt(0, 0).unwrap(),
            execution_payloads: vec![json!({"foo": "bar"})],
            slot: Slot(1),
        };

        // The size of slot is 4 bytes, the timestamp is 8 bytes, and the payload is 13 bytes.
        // So, the total size should be 4 + 8 + 13 = 25
        assert_eq!(slot_bundle.size_in_bytes(), 25);
    }
}
