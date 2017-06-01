extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;

use std::io;
use std::marker::PhantomData;

#[derive(Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Debug, Clone, Copy)]
enum NodeType {
    Internal,
    Leaf,
}

impl Default for NodeType {
    fn default() -> NodeType { NodeType::Internal }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
struct Node<K> {
    node_type: NodeType,
    // Each of these is the largest key found in the subtree.
    children: Vec<(K, u64)>,
    final_child: u64,
    #[serde(skip)]
    file_offset: Option<u64>,
}

impl<K: Copy+Ord+serde::de::DeserializeOwned> Node<K> {

    fn from_reader<R: io::Read+io::Seek>(reader: &mut R, offset: u64) -> bincode::Result<Node<K>> {
        reader.seek(io::SeekFrom::Start(offset))
        .map_err(
            |x| Box::new(bincode::internal::ErrorKind::IoError(x))
        )?;
        let mut res: Node<K> = bincode::deserialize_from(reader, bincode::Infinite)?;
        res.file_offset = Some(offset);
        Ok(res)
    }


    /// Load and return leaf node which may contain the given key.
    fn find_leaf<R: io::Read+io::Seek>(&self, reader: &mut R, key: &K) -> bincode::Result<Node<K>> {
        if self.node_type == NodeType::Leaf { return Ok(self.clone()); }
        // The traditional btree algorithm finds the first key greater than the one we want, then returns the one before it.
        // Our nodes will be quite large, so it's better to binary search.
        let found = self.children.binary_search_by_key(&key, |x| &x.0);
        let child_offset = match found {
            Ok(index) => {
                // The keys in the vec are the highest key in the subtree. If we exactly match, it's this subtree.
                self.children[index].1
            }
            Err(index) => {
                // Otherwise, we get an index between two subtrees.
                // The key is greater than the element this index points at, but the vec is storing maximums.
                let wanted = index+1;
                self.children.get(wanted).map(|x| x.1).unwrap_or(self.final_child)
            }
        };
        // Load and recurse.
        let n = Node::from_reader(reader, child_offset)?;
        n.find_leaf(reader, key)
    }

    /// Does this subtree contain the key? If so, what's the offset?
    fn offset_for_key<R: io::Read+io::Seek>(&self, reader: &mut R, key: &K) -> bincode::Result<Option<u64>> {
        if self.node_type == NodeType::Leaf {
            Ok(self.scan(key))
        } else {
            Ok(self.find_leaf(reader, key)?.scan(key))
        }
    }

    fn scan(&self, key: &K) -> Option<u64> {
        assert_eq!(self.node_type, NodeType::Leaf);
        if let Ok(ind) = self.children.binary_search_by_key(key, |x| x.0) {
            Some(self.children[ind].1)
        } else {
            None
        }
    }
}

/** A B+ tree backed by some storage method.

Internally, this B+ tree uses a log file format, and only ever appends.  All modifying operations return a new BPTree.  All previous versions of the tree remain valid.

In order to make this work, B+ trees do not take references to their readers.  Instead, all functions take a reference.  Always use the same one save for compacting, in which case the destination reader is the reader associated with the tree.

Two BPTrees are equivalent if they have the same offset and are ordered by offset in the backing storage.

It is safe for multiple trees to share the same backing storage.  It is safe to write arbitrary data to the backing storage.

The serde-serialized representation of a BPTree is the offset in the backing storage for the tree.  You will need to keep the backing storage.  This exists to make use in headers more convenient, not for general storage.*/
#[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct BPTree<K, V> {
    offset: u64,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> BPTree<K, V>
where K: Copy+Ord+serde::Serialize+serde::de::DeserializeOwned,
V: Copy+Ord+serde::Serialize+serde::de::DeserializeOwned, {
    pub fn new() -> BPTree<K, V> {
        BPTree::from_offset(0)
    }

    pub fn from_offset(offset: u64) -> BPTree<K, V> {
        BPTree { offset, _phantom: Default::default() }
    }

    pub fn contains<R: io::Read+io::Seek>(&self, reader: &mut R, key: &K) -> bincode::Result<bool> {
        let root = self.get_root(reader)?;
        Ok(root.offset_for_key(reader, key)?.is_some())
    }

    fn get_root<R: io::Read+io::Seek>(&self, reader: &mut R) -> bincode::Result<Node<K>> {
        Node::from_reader(reader, self.offset)
    }
}

impl<K, V> serde::Serialize for BPTree<K, V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.offset.serialize(serializer)
    }
}

impl<'de, K, V> serde::Deserialize<'de> for BPTree<K, V> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let offset = <u64 as serde::Deserialize<'de>>::deserialize(deserializer)?;
        Ok(BPTree { offset, _phantom: Default::default() })
    }
}
