use std::marker::PhantomData;

#[derive(Debug)]
pub struct Entries<K, V> {
    entries: Vec<V>,
    _phantom: PhantomData<K>,
}

impl<K, V> Entries<K, V> {
    pub fn new() -> Self {
        Self {
            entries: Vec::with_capacity(0),
            _phantom: PhantomData,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entries: Vec::with_capacity(cap),
            _phantom: PhantomData,
        }
    }
}

impl<K, T> Entries<K, T>
where
    K: Into<usize>,
{
    pub fn get(&self, key: K) -> Option<&T> {
        self.entries.get(key.into())
    }

    pub fn get_mut(&mut self, key: K) -> Option<&mut T> {
        self.entries.get_mut(key.into())
    }
}

impl<K, T> Entries<K, T>
where
    K: From<usize>,
{
    pub fn push(&mut self, value: T) -> K {
        let current = self.entries.len();
        self.entries.push(value);
        K::from(current)
    }
}

impl<K, V> Entries<K, V>
where
    K: Into<usize>,
    K: From<usize>,
    V: Default,
{
    pub fn push_with<F>(&mut self, fun: F) -> K
    where
        F: FnOnce(&mut V),
    {
        let current = self.entries.len();
        self.entries.push(Default::default());
        fun(&mut self.entries[current]);
        K::from(current)
    }

    pub fn insert_at(&mut self, key: K, value: V) {
        let index: usize = key.into();

        if index >= self.entries.len() {
            self.entries.resize_with(index + 1, V::default);
        }

        self.entries[index] = value;
    }
}

impl<K, V> std::ops::Index<K> for Entries<K, V>
where
    K: Into<usize>,
{
    type Output = V;

    fn index(&self, index: K) -> &Self::Output {
        &self.entries[index.into()]
    }
}

impl<K, V> std::ops::IndexMut<K> for Entries<K, V>
where
    K: Into<usize>,
{
    fn index_mut(&mut self, index: K) -> &mut Self::Output {
        &mut self.entries[index.into()]
    }
}
