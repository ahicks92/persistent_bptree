use std;
use std::io::{self, Read, Seek, Write};
use std::error::Error;
use std::cell;
use bincode;
use serde;


#[derive(Debug)]
pub enum DecodingError {
    Corrupt(String),
    IoError(io::Error),
}

fn decode<O: serde::de::DeserializeOwned, R: Read+Seek>(reader: &mut R, offset: u64) -> Result<O, DecodingError> {
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

fn encode<O: serde::Serialize, W: Write+Seek>(writer: &mut W, obj: &O) -> Result<u64, EncodingError> {
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
    fn load<R: Read+Seek>(reader: &mut R, offset: u64) -> Result<DiskNode<K>, DecodingError> {
        decode(reader, offset)        
    }
}

fn load<K: serde::de::DeserializeOwned, R: Read+Seek>(reader: &mut R, offset: u64) -> Result<Node<K>, DecodingError> {
    Ok(DiskNode::<K>::load(reader, offset)?.into())
}

impl<K: serde::de::DeserializeOwned> NodeRef<K> {
    fn from_offset(offset: u64) -> NodeRef<K> {
        NodeRef(cell::UnsafeCell::new(NodeRefInternal::Unloaded(offset)))
    }

    fn from_boxed_node(node: Box<Node<K>>) -> NodeRef<K> {
        NodeRef(cell::UnsafeCell::new(NodeRefInternal::Loaded(node)))
    }

    fn load<R: Read+Seek>(&self, reader: &mut R) -> Result<(), DecodingError> {
        let internal = self.0.get();
        unsafe {
            if let NodeRefInternal::Unloaded(offset) = *internal {
                *internal = NodeRefInternal::Loaded(Box::new(load(reader, offset)?));
            }
        }
        Ok(())
    }

    fn get<R: Read+Seek>(&self, reader: &mut R) -> Result<&Node<K>, DecodingError> {
        self.load(reader)?;
        unsafe {
            Ok(match *self.0.get() {
                NodeRefInternal::Loaded(ref n) => n,
                _ => panic!("Nodes should be loaded."),
            })
        }
    }

    fn get_mut<R: Read+Seek>(&mut self, reader: &mut R) -> Result<&mut Node<K>, DecodingError> {
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

    fn into_box<R: Read+Seek>(self, reader: &mut R) -> Result<Box<Node<K>>, DecodingError> {
        self.load(reader)?;
        let old = unsafe {
            std::mem::replace(&mut *self.0.get(), NodeRefInternal::Unloaded(0))
        };
        match old {
            NodeRefInternal::Loaded(n) => Ok(n),
            _ => panic!("Somehow, this is an unloaded node."),
        }
    }
}

impl<K> Drop for NodeRef<K> {
    fn drop(&mut self) {
        unsafe { std::ptr::drop_in_place(self.0.get()); }
    }
}

impl<K: serde::de::DeserializeOwned+Eq+Ord+Clone> Node<K> {
    fn find_offset_for<R: Read+Seek>(&self, reader: &mut R, key: &K) -> Result<Option<u64>, DecodingError> {
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

    /// Modify this node in place to split in half, returning the upper half and the dividing key.
    fn split_in_place(&mut self) -> (K, Box<Node<K>>) {
        // Doing this based off keys is important.
        let half = self.keys.len()/2;
        let upper_keys = self.keys.drain(half..).collect::<Vec<_>>();
        let upper_children = self.children.drain(half..).collect::<Vec<_>>();
        assert!(self.children.len() > 1);
        assert!(self.keys.len() > 1);
        assert!(upper_children.len() > 1);
        assert!(upper_keys.len() > 1);
        let ret_key;
        let ret_node;
        match self.node_type {
            NodeType::Leaf => {
                ret_key = self.keys.last().unwrap().clone();
                ret_node = Node {
                    keys: upper_keys,
                    children: upper_children,
                    node_type: NodeType::Leaf,
                    modified: true,
                };
            }
            NodeType::Root | NodeType::Internal => {
                // A split of the root makes us an internal, and something else will construct the new root.
                self.node_type = NodeType::Internal;
                // We have one extra key in ourself right now. This is greater than any value beneath us.
                // In this implementation we go left for <=.
                ret_key = self.keys.pop().unwrap();
                ret_node = Node {
                    node_type: NodeType::Internal,
                    keys: upper_keys,
                    children: upper_children,
                    modified: true,
                }
            }
        }
        (ret_key, Box::new(ret_node))
    }

    fn insert_nonroot<R: Read+Seek>(&mut self, reader: &mut R, key: &K, value: u64, split_threshold: usize)
        -> Result<Option<(K, Box<Node<K>>)>, DecodingError>
    {
        assert!(self.node_type != NodeType::Root);
        let target = self.index_of(key);
        if self.node_type == NodeType::Leaf {
            if &self.keys[target] == key {
                self.children[target] = NodeRef::from_offset(value);
            }
            else {
                self.keys.insert(target, key.clone());
                self.children.insert(target, NodeRef::from_offset(value));
            }
        }
        else {
            let needs_split = self.children[target].get_mut(reader)?.insert_nonroot(reader, key, value, split_threshold)?;
            if let Some((k, n)) = needs_split {
                // This makes the new key "our" new maximum.
                self.keys.insert(target, k);
                // The new node is between the new key and the one after it; note the +1.
                // This works because no node is permitted to have less than 2 children.
                self.children.insert(target+1, NodeRef::from_boxed_node(n));
            }
        }
        if self.children.len() > split_threshold {
            Ok(Some(self.split_in_place()))
        }
        else { Ok(None) }   
    }

    /// If the root splits, sets our type to internal and/or leaf depending, then returns the new sibling.
    fn insert<R: Read+Seek>(&mut self, reader: &mut R, key: &K, value: u64, order: u64) -> Result<Option<(K, Box<Node<K>>)>, DecodingError> {
        let split_threshold = (order/2+order%2) as usize;
        let target = self.index_of(key);
        let needs_split = self.children[target].get_mut(reader)?.insert_nonroot(reader, key, value, split_threshold)?;
        if let Some((k, n)) = needs_split {
            // Same as insert_nonroot.
            self.keys.insert(target, k);
            self.children.insert(target+1, NodeRef::from_boxed_node(n));
        }
        if self.children.len() > split_threshold {
            let new_type = match self.node_type {
                NodeType::Leaf => NodeType::Leaf,
                NodeType::Root => NodeType::Internal,
                _ => panic!("Should be a root or a lweaf."),
            };
            self.node_type = new_type;
            Ok(Some(self.split_in_place()))
        }
        else { Ok(None) }
    }
}

struct OffsetTree<K> {
    root_reference: NodeRef<K>,
    order: u64,
}

impl<K: serde::de::DeserializeOwned+Eq+Ord+Clone> OffsetTree<K> {
    pub fn from_root_offset(offset: u64, order: u64) -> OffsetTree<K> {
        OffsetTree {
            root_reference: NodeRef::from_offset(offset),
            order,
        }
    }

    pub fn contains<R: Read+Seek>(&mut self, reader: &mut R, key: &K) -> Result<bool, DecodingError> {
        Ok(self.offset_for(reader, key)?.is_some())
    }

    pub fn offset_for<R: Read+Seek>(&mut self, reader: &mut R, key: &K) -> Result<Option<u64>, DecodingError> {
        self.root_reference.get(reader)?.find_offset_for(reader, key)
    }

    pub fn insert<R: Read+Seek>(&mut self, reader: &mut R, key: &K, value: u64) -> Result<(), DecodingError> {
        let needs_split = self.root_reference.get_mut(reader)?.insert(reader, key, value, self.order)?;
        if let Some((k, right)) = needs_split {
            // This is a hack to get around moving out.
            let r = std::mem::replace(&mut self.root_reference, NodeRef::from_offset(0));
            let left = r.into_box(reader)?;
            let new_node = Node {
                node_type: NodeType::Root,
                modified: true,
                keys: vec![k],
                children: vec![NodeRef::from_boxed_node(left), NodeRef::from_boxed_node(right)],
            };
            self.root_reference = NodeRef::from_boxed_node(Box::new(new_node));
        }        
        Ok(())
    }
}
