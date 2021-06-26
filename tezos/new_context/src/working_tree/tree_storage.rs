use std::{borrow::Borrow, convert::TryInto, num::NonZeroU32};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TreeStorageId {
    start: u32,
    end: NonZeroU32,
}

impl Default for TreeStorageId {
    fn default() -> Self {
        Self::empty()
    }
}

impl TreeStorageId {
    fn new(start: usize, end: usize) -> Self {
        Self {
            start: start.try_into().unwrap(),
            end: NonZeroU32::new(end.checked_add(1).unwrap().try_into().unwrap()).unwrap(),
        }
    }

    fn get(self) -> (usize, usize) {
        (
            self.start as usize,
            self.end.get().checked_sub(1).unwrap() as usize,
        )
    }

    pub fn empty() -> Self {
        Self::new(0, 0)
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end.get() - 1
    }
}

pub struct TreeStorage<K, V>
where
    K: Ord,
{
    trees: Vec<(K, V)>,
    temp_vec: Vec<(K, V)>,
}

impl<K, V> Default for TreeStorage<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> TreeStorage<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            trees: Vec::with_capacity(1024),
            temp_vec: Vec::with_capacity(128),
        }
    }

    pub fn new_tree(&self) -> TreeStorageId {
        TreeStorageId::new(0, 0)
    }

    pub fn get_tree(&self, tree_id: TreeStorageId) -> Option<&[(K, V)]> {
        let (start, end) = tree_id.get();

        self.trees.get(start..end)
    }

    pub fn get<BK>(&self, tree_id: TreeStorageId, key: &BK) -> Option<&V>
    where
        BK: Ord + ?Sized,
        K: Borrow<BK>,
    {
        let tree = self.get_tree(tree_id)?;

        let key = key.borrow();
        let index = tree
            .binary_search_by(|value| value.0.borrow().cmp(key))
            .ok()?;

        tree.get(index).map(|v| &v.1)
    }

    fn add_tree_with<F>(&mut self, fun: F) -> TreeStorageId
    where
        F: FnOnce(&mut Vec<(K, V)>),
    {
        let start = self.trees.len();
        fun(&mut self.trees);
        let end = self.trees.len();

        TreeStorageId::new(start, end)
    }

    pub fn add_tree_with_result<F, E>(&mut self, fun: F) -> Result<TreeStorageId, E>
    where
        F: FnOnce(&mut Vec<(K, V)>) -> Result<(), E>,
    {
        let start = self.trees.len();
        let result = fun(&mut self.trees);
        let end = self.trees.len();

        let tree_id = TreeStorageId::new(start, end);
        result.map(|_| tree_id)
    }

    /// Use `self.temp_vec` to avoid 1 allocation in insert/remove
    fn with_temporary_tree<F, R>(&mut self, fun: F) -> R
    where
        F: FnOnce(&mut Self, &mut Vec<(K, V)>) -> R,
    {
        let mut new_tree = std::mem::take(&mut self.temp_vec);
        new_tree.clear();

        let result = fun(self, &mut new_tree);

        self.temp_vec = new_tree;
        result
    }

    pub fn insert<Key>(&mut self, tree_id: TreeStorageId, key: Key, value: V) -> TreeStorageId
    where
        Key: Into<K>,
    {
        let key = key.into();

        self.with_temporary_tree(|this, new_tree| {
            let tree = match this.get_tree(tree_id) {
                Some(tree) if !tree.is_empty() => tree,
                _ => {
                    return this.add_tree_with(|trees| {
                        trees.push((key, value));
                    });
                }
            };

            let index = tree.binary_search_by(|value| value.0.cmp(&key));

            match index {
                Ok(found) => {
                    new_tree.extend_from_slice(tree);
                    new_tree[found].1 = value;
                }
                Err(index) => {
                    new_tree.extend_from_slice(&tree[..index]);
                    new_tree.push((key, value));
                    new_tree.extend_from_slice(&tree[index..]);
                }
            }

            this.add_tree_with(|trees| {
                trees.append(new_tree);
            })
        })
    }

    pub fn remove<BK>(&mut self, tree_id: TreeStorageId, key: &BK) -> TreeStorageId
    where
        BK: Ord + ?Sized,
        K: Borrow<BK>,
    {
        self.with_temporary_tree(|this, new_tree| {
            let tree = match this.get_tree(tree_id) {
                Some(tree) if !tree.is_empty() => tree,
                _ => {
                    return tree_id;
                }
            };

            let key = key.borrow();
            let index = match tree.binary_search_by(|value| value.0.borrow().cmp(&key)) {
                Ok(index) => index,
                Err(_) => {
                    return tree_id;
                }
            };

            if index > 0 {
                new_tree.extend_from_slice(&tree[..index]);
            }
            if index + 1 != tree.len() {
                new_tree.extend_from_slice(&tree[index + 1..]);
            }

            this.add_tree_with(|trees| {
                trees.append(new_tree);
            })
        })
    }

    pub fn clear(&mut self) {
        *self = Self::new()
    }

    pub fn iter(&self, tree_id: TreeStorageId) -> TreeIter<K, V> {
        self.get_tree(tree_id)
            .map(|tree| TreeIter { tree, current: 0 })
            .unwrap()
    }

    pub fn values(&self, tree_id: TreeStorageId) -> TreeValues<K, V> {
        self.get_tree(tree_id)
            .map(|tree| TreeValues { tree, current: 0 })
            .unwrap()
    }

    pub fn consuming_iter(&self, tree_id: TreeStorageId) -> TreeConsumingIter<K, V> {
        self.get_tree(tree_id)
            .map(|tree| TreeConsumingIter {
                tree: tree.to_vec(),
                current: 0,
            })
            .unwrap()
    }
}

pub struct TreeIter<'a, K, V>
where
    K: Ord,
{
    tree: &'a [(K, V)],
    current: usize,
}

impl<'a, K, V> Iterator for TreeIter<'a, K, V>
where
    K: Ord,
{
    type Item = &'a (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.tree.get(self.current);
        self.current += 1;
        current
    }
}

pub struct TreeConsumingIter<K, V> {
    tree: Vec<(K, V)>,
    current: usize,
}

impl<K, V> Iterator for TreeConsumingIter<K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.tree.get(self.current).cloned();
        self.current += 1;
        current
    }
}

pub struct TreeValues<'a, K, V>
where
    K: Ord,
{
    tree: &'a [(K, V)],
    current: usize,
}

impl<'a, K, V> Iterator for TreeValues<'a, K, V>
where
    K: Ord,
{
    type Item = &'a V;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.tree.get(self.current).map(|v| &v.1);
        self.current += 1;
        current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tree_storage() {
        println!("ID={:?}", std::mem::size_of::<TreeStorageId>());
        println!(
            "Option<TreeStorageId>={:?}",
            std::mem::size_of::<Option<TreeStorageId>>()
        );
        println!(
            "Option<Option<TreeStorageId>>={:?}",
            std::mem::size_of::<Option<Option<TreeStorageId>>>()
        );

        let mut tree_storage = TreeStorage::new();

        let tree_id = tree_storage.new_tree();
        let tree_id = tree_storage.insert(tree_id, "a", 1);
        let tree_id = tree_storage.insert(tree_id, "b", 2);
        let tree_id = tree_storage.insert(tree_id, "0", 3);

        assert_eq!(
            tree_storage.get_tree(tree_id).unwrap(),
            &[("0", 3), ("a", 1), ("b", 2),]
        );

        println!("TREE={:?}", tree_storage.get_tree(tree_id));
    }
}
