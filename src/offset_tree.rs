use std;
use serde;
use std::cell;
use storage_backend::StorageBackend;

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
    fn load<B: StorageBackend>(backend: &B, offset: u64) -> Result<DiskNode<K>, B::DecodingError> {
        backend.load(offset)        
    }
}

fn load<K: serde::de::DeserializeOwned, B: StorageBackend>(backend: &B, offset: u64) -> Result<Node<K>, B::DecodingError> {
    Ok(DiskNode::<K>::load(backend, offset)?.into())
}

impl<K: serde::de::DeserializeOwned> NodeRef<K> {
    fn from_offset(offset: u64) -> NodeRef<K> {
        NodeRef(cell::UnsafeCell::new(NodeRefInternal::Unloaded(offset)))
    }

    fn from_boxed_node(node: Box<Node<K>>) -> NodeRef<K> {
        NodeRef(cell::UnsafeCell::new(NodeRefInternal::Loaded(node)))
    }

    fn load<B: StorageBackend>(&self, backend: &B) -> Result<(), B::DecodingError> {
        let internal = self.0.get();
        unsafe {
            if let &NodeRefInternal::Unloaded(offset) = &*internal {
                *internal = NodeRefInternal::Loaded(Box::new(load(backend, offset)?));
            }
        }
        Ok(())
    }

    fn get<B: StorageBackend>(&self, backend: &B) -> Result<&Node<K>, B::DecodingError> {
        self.load(backend)?;
        unsafe {
            Ok(match *self.0.get() {
                NodeRefInternal::Loaded(ref n) => n,
                _ => panic!("Nodes should be loaded."),
            })
        }
    }

    fn get_mut<B: StorageBackend>(&mut self, backend: &B) -> Result<&mut Node<K>, B::DecodingError> {
        self.load(backend)?;
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

    fn into_box<B: StorageBackend>(self, backend: &B) -> Result<Box<Node<K>>, B::DecodingError> {
        self.load(backend)?;
        let ret = unsafe {
            match std::ptr::read(self.0.get()) {
                NodeRefInternal::Loaded(n) => Ok(n),
                _ => panic!("Somehow, this is an unloaded node."),
            }
        };
        std::mem::forget(self);
        ret
    }
}

impl<K: serde::de::DeserializeOwned+Eq+Ord+Clone> Node<K> {
    fn find_offset_for<B: StorageBackend>(&self, backend: &B, key: &K) -> Result<Option<u64>, B::DecodingError> {
        if self.node_type == NodeType::Leaf {
            match self.keys.binary_search(key) {
                Ok(ind) => Ok(Some(self.children[ind].offset_or_panic("This is a leaf, but somehow has a loaded child."))),
                Err(_) => Ok(None),
            }
        }
        else {
            self.children[self.index_of(key)].get(backend)?.find_offset_for(backend, key)
        }
    }

    fn index_of(&self, key: &K) -> usize{
        assert!(self.node_type != NodeType::Leaf);
        let ind = self.keys.binary_search(key);
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
        let d = if self.node_type == NodeType::Leaf { 0 } else { 1 };
        assert_eq!(self.keys.len()+d, self.children.len());
        assert_eq!(ret_node.keys.len()+d, ret_node.children.len());
        assert!(self.keys.last().unwrap() <= &ret_key);
        assert!(&ret_key < ret_node.keys.first().unwrap());
        (ret_key, Box::new(ret_node))
    }

    fn insert_nonroot<B: StorageBackend>(&mut self, backend: &B, key: &K, value: u64, split_threshold: usize)
        -> Result<Option<(K, Box<Node<K>>)>, B::DecodingError>
    {
        assert!(self.node_type != NodeType::Root);
        if self.node_type == NodeType::Leaf {
            match self.keys.binary_search(key) {
                Ok(ind) => {
                    self.children[ind] = NodeRef::from_offset(value);
                },
                Err(ind) => {
                    self.keys.insert(ind, key.clone());
                    self.children.insert(ind, NodeRef::from_offset(value));
                }
            }
        }
        else {
            let target = self.index_of(key);
            let needs_split = self.children[target].get_mut(backend)?.insert_nonroot(backend, key, value, split_threshold)?;
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
    fn insert<B: StorageBackend>(&mut self, backend: &B, key: &K, value: u64, order: u64) -> Result<Option<(K, Box<Node<K>>)>, B::DecodingError> {
        let split_threshold = (order/2+order%2) as usize;
        // Leaf is a special, short-circuiting case:
        if self.node_type == NodeType::Leaf {
            return self.insert_nonroot(backend, key, value, split_threshold);
        }
        let target = self.index_of(key);
        let needs_split = self.children[target].get_mut(backend)?.insert_nonroot(backend, key, value, split_threshold)?;
        if let Some((k, n)) = needs_split {
            // Same as insert_nonroot.
            self.keys.insert(target, k);
            self.children.insert(target+1, NodeRef::from_boxed_node(n));
        }
        if self.children.len() > split_threshold {
            Ok(Some(self.split_in_place()))
        }
        else { Ok(None) }
    }
}

pub struct OffsetTree<K> {
    root_reference: NodeRef<K>,
    order: u64,
}

impl<K: serde::de::DeserializeOwned+Eq+Ord+Clone> OffsetTree<K> {
    pub fn empty(order: u64) -> OffsetTree<K> {
        let initial_leaf = Box::new(Node {
            modified: true,
            node_type: NodeType::Leaf,
            keys: vec![],
            children: vec![],
        });
        OffsetTree {
            root_reference: NodeRef::from_boxed_node(initial_leaf),
            order,
        }
    }

    pub fn from_root_offset(offset: u64, order: u64) -> OffsetTree<K> {
        OffsetTree {
            root_reference: NodeRef::from_offset(offset),
            order,
        }
    }

    pub fn contains<B: StorageBackend>(&mut self, backend: &B, key: &K) -> Result<bool, B::DecodingError> {
        Ok(self.offset_for(backend, key)?.is_some())
    }

    pub fn offset_for<B: StorageBackend>(&mut self, backend: &B, key: &K) -> Result<Option<u64>, B::DecodingError> {
        self.root_reference.get(backend)?.find_offset_for(backend, key)
    }

    pub fn insert<B: StorageBackend>(&mut self, backend: &B, key: &K, value: u64) -> Result<(), B::DecodingError> {
        let needs_split = self.root_reference.get_mut(backend)?.insert(backend, key, value, self.order)?;
        if let Some((k, right)) = needs_split {
            // This is a hack to get around moving out.
            let r = std::mem::replace(&mut self.root_reference, NodeRef::from_offset(0));
            let left = r.into_box(backend)?;
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
