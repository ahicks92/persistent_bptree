#![allow(dead_code)]
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate take_mut;

use std::io;
use std::error::Error;
use std::marker::PhantomData;
use std::cell;


#[derive(Debug)]
pub enum DecodingError {
    Corrupt(String),
    IoError(io::Error),
}

fn decode<O: serde::de::DeserializeOwned, R: io::Read+io::Seek>(reader: &mut R, offset: u64) -> Result<O, DecodingError> {
    reader.seek(io::SeekFrom::Start(offset)).map_err(|x| DecodingError::IoError(x))?;
    bincode::deserialize_from(    reader, bincode::Infinite)
    .map_err(|x| {
        match *x {
            bincode::ErrorKind::IoError(y) => DecodingError::IoError(y),
            _ => DecodingError::Corrupt(x.description().to_string()),
        }
    })
}

#[derive(Debug)]
pub enum EncodingError {
    Unknown(String),
    IoError(io::Error),
}

fn encode<O: serde::Serialize, W: io::Write+io::Seek>(writer: &mut W, obj: &O) -> Result<u64, EncodingError> {
    let offset = writer.seek(io::SeekFrom::End(0)).map_err(|x| EncodingError::IoError(x))?;
    bincode::serialize_into(writer, obj, bincode::Infinite)
    .map_err(|x| {
        match *x {
            bincode::ErrorKind::IoError(y) => EncodingError::IoError(y),
            _ => EncodingError::Unknown(x.description().to_string()),
        }
    }).map(|_| offset)
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum NodeType {
    Root,
    Internal,
    Leaf
}

enum NodeRefInternal<K> {
    Unloaded(u64),
    Loaded(Box<Node<K>>),
}

struct NodeRef<K>(cell::UnsafeCell<NodeRefInternal<K>>);

// An on-disk representation, for space saving.
#[derive(Serialize, Deserialize)]
struct DiskNode<K> {
    node_type: NodeType,
    keys: Vec<K>,
    children: Vec<u64>,
}

struct Node<K> {
    node_type: NodeType,
    keys: Vec<K>,
    children: Vec<NodeRef<K>>,
    modified: bool,
}

impl<K: serde::de::DeserializeOwned> From<DiskNode<K>> for Node<K> {
    fn from(obj: DiskNode<K>) -> Node<K> {
        Node {
            node_type: obj.node_type,
            keys: obj.keys,
            children: obj.children.into_iter().map(|x| (NodeRef::from_offset(x))).collect(),
            modified: false,
        }
    }
}

impl<K: serde::de::DeserializeOwned> DiskNode<K> {
    fn load<R: io::Read+io::Seek>(reader: &mut R, offset: u64) -> Result<DiskNode<K>, DecodingError> {
        decode(reader, offset)        
    }
}

fn load<K: serde::de::DeserializeOwned, R: io::Read+io::Seek>(reader: &mut R, offset: u64) -> Result<Node<K>, DecodingError> {
    Ok(DiskNode::<K>::load(reader, offset)?.into())
}

impl<K: serde::de::DeserializeOwned> NodeRef<K> {
    fn from_offset(offset: u64) -> NodeRef<K> {
        NodeRef(cell::UnsafeCell::new(NodeRefInternal::Unloaded(offset)))
    }

    fn load<R: io::Read+io::Seek>(&self, reader: &mut R) -> Result<(), DecodingError> {
        let internal = self.0.get();
        unsafe {
            if let NodeRefInternal::Unloaded(offset) = *internal {
                *internal = NodeRefInternal::Loaded(Box::new(load(reader, offset)?));
            }
        }
        Ok(())
    }

    fn get<R: io::Read+io::Seek>(&self, reader: &mut R) -> Result<&Node<K>, DecodingError> {
        self.load(reader)?;
        unsafe {
            Ok(match *self.0.get() {
                NodeRefInternal::Loaded(ref n) => n,
                _ => panic!("Nodes should be loaded."),
            })
        }
    }

    fn get_mut<R: io::Read+io::Seek>(&mut self, reader: &mut R) -> Result<&mut Node<K>, DecodingError> {
        self.load(reader)?;
        unsafe {
            Ok(match *self.0.get() {
                NodeRefInternal::Loaded(ref mut n) => {
                    n.modified = true;
                    n
                },
                _ => panic!("Node not loaded."),
            })
        }
    }

    fn offset_or_panic(&self, msg: &'static str) -> u64 {
        let internal = self.0.get();
        unsafe {
            match *internal {
                NodeRefInternal::Unloaded(offset) => offset,
                _ => panic!(msg),
            }
        }
    }
}

impl<K> Drop for NodeRef<K> {
    fn drop(&mut self) {
        unsafe { std::ptr::drop_in_place(self.0.get()); }
    }
}

impl<K: serde::de::DeserializeOwned+Eq+Ord> Node<K> {
    fn find_offset_for<R: io::Read+io::Seek>(&self, reader: &mut R, key: &K) -> Result<Option<u64>, DecodingError> {
        if self.node_type == NodeType::Leaf {
            if self.children.len() == 0 { return Ok(None) } //empty tree.
            match self.keys.binary_search(key) {
                Ok(ind) => Ok(Some(self.children[ind].offset_or_panic("This is a leaf, but somehow has a loaded child."))),
                Err(_) => Ok(None),
            }
        }
        else {
            self.children[self.index_of(key)].get(reader)?.find_offset_for(reader, key)
        }
    }

    fn index_of(&self, key: &K) -> usize{
        assert!(self.node_type != NodeType::Leaf);
        let ind = self.keys.binary_search(key);
        // Explanation: this returns the index of the child to the right of the last key smaller than key.
        // In this implementation, before is <= and after is >.
        // Before is the index of the key, after is the index of the key+1.
        // The not found case of binary_search is the key+1.
        match ind {
            Ok(index) | Err(index) => index,
        }
    }
}

struct BPTree<'a, R: 'a, K, V> {
    root_reference: NodeRef<K>,
    backing_io: &'a mut R,
    _phantom: PhantomData<V>
}

impl<'a, R: io::Read+io::Seek, K: serde::de::DeserializeOwned+Eq+Ord, V> BPTree<'a, R, K, V> {
    pub fn contains(&mut self, key: &K) -> Result<bool, DecodingError> {
        Ok(self.offset_for(key)?.is_some())
    }

    pub fn offset_for(&mut self, key: &K) -> Result<Option<u64>, DecodingError> {
        self.root_reference.get(self.backing_io)?.find_offset_for(self.backing_io, key)
    }
}
