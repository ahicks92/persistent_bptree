extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;

use std::io;

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
    file_offset: u64,
}

impl<'de, K: Copy+Ord+serde::de::DeserializeOwned> Node<K> {

    fn from_reader<R: io::Read+io::Seek>(offset: u64, reader: &mut R) -> bincode::Result<Node<K>> {
        reader.seek(io::SeekFrom::Start(offset))
        .map_err(
            |x| Box::new(bincode::internal::ErrorKind::IoError(x))
        )?;
        bincode::deserialize_from(reader, bincode::Infinite)
    }


    /// Load and return leaf node which may contain the given key.
    /// Consumers of this API need to clone beforehand because we can't pass out a reference to the underlying file and must act as though it's always a new object.
    /// We don't want to force this function to always pay the penalty of cloning the vec.
    fn find_leaf<R: io::Read+io::Seek>(self, key: &K, reader: &mut R) -> bincode::Result<Node<K>> {
        if self.node_type == NodeType::Leaf { return Ok(self); }
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
        let n = Node::from_reader(child_offset, reader)?;
        n.find_leaf(key, reader)
    }

}