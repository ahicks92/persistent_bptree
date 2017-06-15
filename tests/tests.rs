extern crate append_tree;
use append_tree::StorageBackend;
extern crate rand;
extern crate bincode;
extern crate serde;
use rand::{XorShiftRng, Rng, SeedableRng};
use append_tree::offset_tree::OffsetTree;
use std::collections::HashMap;

#[derive(Default)]
struct MemoryBackend {
    map: HashMap<u64, Vec<u8>>,
    count: u64,
}

impl MemoryBackend {
    fn new() -> MemoryBackend {
        Default::default()
    }
}

impl StorageBackend for MemoryBackend {
    type DecodingError = Box<bincode::ErrorKind>;
    type EncodingError = Box<bincode::ErrorKind>;

    fn load<V: serde::de::DeserializeOwned>(&self, key: u64) -> Result<V, Self::DecodingError> {
        let v = self.map.get(&key).unwrap();
        bincode::deserialize(&v)
    }

    fn store<V: serde::Serialize>(&mut self, value: &V) -> Result<u64, Self::EncodingError> {
        let key = self.count;
        let serialized = bincode::serialize(value, bincode::Infinite)?;
        // We should never end up inserting a duplicate key.
        assert!(self.map.insert(key, serialized) == None);
        self.count += 1;
        Ok(key)
    }
}

#[test]
fn test_insertion_nocommit() {
    let mut rng = XorShiftRng::from_seed([1, 1, 1, 1]);
    let count = 10000;
    let order = 7;
    let mut points = (0..count).zip(rng.gen_iter().take(count)).collect::<Vec<_>>();
    rng.shuffle(&mut points);
    let mut tree = OffsetTree::empty(order);
    let backend = MemoryBackend::new();
    for &(ref k, ref v) in points.iter() {
        tree.insert(&backend, k, *v).unwrap();
    }
    for &(ref k, ref v) in points.iter() {
        let got = tree.offset_for(&backend, k).unwrap().unwrap();
        assert_eq!(got,  *v);
    }
}

