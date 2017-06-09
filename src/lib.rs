#![allow(dead_code)]
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate take_mut;

use std::io;
use std::error::Error;
use std::marker::PhantomData;


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

#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum NodeType {
    Root,
    Internal,
    Leaf
}

enum NodeRef<K> {
    Unloaded(u64),
    Loaded(Box<Node<K>>),
    Modified(Box<Node<K>>),
}

// An on-disk representation, for space saving.
#[derive(Serialize, Deserialize)]
struct DiskNode<K> {
    node_type: NodeType,
    children: Vec<(K, u64)>,
    after_last: u64,
}

struct Node<K> {
    node_type: NodeType,
    children: Vec<(K, NodeRef<K>)>,
    after_last: NodeRef<K>,
}

impl<K> From<DiskNode<K>> for Node<K> {
    fn from(obj: DiskNode<K>) -> Node<K> {
        Node {
            node_type: obj.node_type,
            children: obj.children.into_iter().map(|x| (x.0, NodeRef::Unloaded(x.1))).collect(),
            after_last: NodeRef::Unloaded(obj.after_last),
        }
    }
}

impl<K: serde::de::DeserializeOwned> DiskNode<K> {
    fn load<R: io::Read+io::Seek>(reader: &mut R, offset: u64) -> Result<DiskNode<K>, DecodingError> {
        decode(reader, offset)        
    }
}

impl<K: serde::de::DeserializeOwned> NodeRef<K> {
    fn load<R: io::Read+io::Seek>(reader: &mut R, offset: u64) -> Result<Node<K>, DecodingError> {
        Ok(DiskNode::<K>::load(reader, offset)?.into())
    }

    // Upgrade through the variants, optionally all the way to mutable.
    fn upgrade<R: io::Read+io::Seek>(&mut self, reader: &mut R, modification: bool) -> Result<(), DecodingError> {
        let new: Option<Box<Node<K>>> = match *self {
            NodeRef::Unloaded(offset) => Some(Box::new(NodeRef::load(reader, offset)?)),
            _ => None,
        };
        // If we have a new one, construct the appropriate variant directly.
        if let Some(b)  = new {
            if modification {
                *self = NodeRef::Modified(b);
            } else {
                *self = NodeRef::Loaded(b);
            }
            return Ok(());
        }
        // If we get here, it's either Loaded or Modified, with a possible need for an upgrade.
        if modification {
            take_mut::take(self, |x| match x {
                NodeRef::Loaded(b) | NodeRef::Modified(b) => NodeRef::Modified(b),
                _ => panic!("The node should be loaded."),
            });
        }
        Ok(())
    }

    fn examine<R: io::Read+io::Seek>(&mut self, reader: &mut R) -> Result<&mut Node<K>, DecodingError> {
        self.upgrade(reader, false)?;
        Ok(match *self {
            NodeRef::Loaded(ref mut n) => n,
            NodeRef::Modified(ref mut n) => n,
            _ => panic!("Nodes should be loaded."),
        })
    }

    fn modify<R: io::Read+io::Seek>(&mut self, reader: &mut R) -> Result<&mut Node<K>, DecodingError> {
        self.upgrade(reader, true)?;
        Ok(match *self {
            NodeRef::Modified(ref mut n) => n,
            _ => panic!("Nodes should be modified."),
        })
    }
}

impl<K: serde::de::DeserializeOwned+Eq+Ord> Node<K> {
    fn find_offset_for<R: io::Read+io::Seek>(&mut self, reader: &mut R, key: &K) -> Result<Option<u64>, DecodingError> {
        // If we're the root and there are no children, this is the empty tree.
        if self.node_type == NodeType::Root && self.children.len() == 0 { Ok(None) }
        else if self.node_type == NodeType::Leaf {
            match self.children.binary_search_by_key(&key, |x| &x.0) {
                Ok(ind) => Ok(Some(match self.children[ind].1 {
                    NodeRef::Unloaded(offset) => offset,
                    _ => panic!("This is supposed to be a leaf, but the child is somehow loaded.")
                })),
                Err(_) => Ok(None),
            }
        }
        else {
            self.step_toward(key).examine(reader)?.find_offset_for(reader, key)
        }
    }

    fn step_toward(&mut self, key: &K) -> &mut NodeRef<K> {
        assert!(self.node_type != NodeType::Leaf);
        let ind = self.children.binary_search_by_key(&key, |x| &x.0);
        match ind {
            // Each subtree <= the key.
            Ok(index) => &mut self.children[index].1,
            Err(index) =>
                if index == self.children.len() { &mut self.after_last } else { &mut self.children[index+1].1 },
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
        self.root_reference.examine(self.backing_io)?.find_offset_for(self.backing_io, key)
    }
}