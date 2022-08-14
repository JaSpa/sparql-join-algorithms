use std::collections;
use std::collections::hash_map::{Entry, RandomState};
use std::hash::{BuildHasher, Hash};
use std::ops::{Deref, DerefMut};

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct HashMap<K, V, S = RandomState>(pub collections::HashMap<K, V, S>);

impl<K, V, S> HashMap<K, V, S> {
    pub fn get(self) -> collections::HashMap<K, V, S> {
        self.0
    }
}

impl<K, V, S> Default for HashMap<K, V, S>
where
    S: Default,
{
    fn default() -> Self {
        collections::HashMap::default().into()
    }
}

impl<K, V, S> From<HashMap<K, V, S>> for collections::HashMap<K, V, S> {
    fn from(m: HashMap<K, V, S>) -> Self {
        m.0
    }
}

impl<K, V, S> From<collections::HashMap<K, V, S>> for HashMap<K, V, S> {
    fn from(m: collections::HashMap<K, V, S>) -> Self {
        HashMap(m)
    }
}

impl<K, V, S> Deref for HashMap<K, V, S> {
    type Target = collections::HashMap<K, V, S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V, S> DerefMut for HashMap<K, V, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V, U, S> Extend<(K, U)> for HashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
    U: Into<V>,
    V: Extend<U>,
{
    fn extend<T: IntoIterator<Item = (K, U)>>(&mut self, iter: T) {
        for (k, u) in iter {
            match self.entry(k) {
                Entry::Vacant(e) => {
                    e.insert(<U as Into<V>>::into(u));
                }
                Entry::Occupied(e) => {
                    e.into_mut().extend_one(u);
                }
            }
        }
    }
}

impl<K, V, U, S> FromIterator<(K, U)> for HashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher + Default,
    U: Into<V>,
    V: Extend<U>,
{
    fn from_iter<T: IntoIterator<Item = (K, U)>>(iter: T) -> Self {
        let mut m = Self::default();
        m.extend(iter);
        m
    }
}

/// Extending a `HashMap` from a stream of `HashMap`s.
impl<K, V, U, S1, S2> Extend<HashMap<K, U, S1>> for HashMap<K, V, S2>
where
    K: Eq + Hash,
    S2: BuildHasher,
    U: Into<V>,
    V: std::iter::Extend<U>,
{
    fn extend<Iter: IntoIterator<Item = HashMap<K, U, S1>>>(&mut self, iter: Iter) {
        for m in iter {
            <Self as Extend<(K, U)>>::extend(self, m.0.into_iter());
        }
    }
}

/// Creating a `HashMap` from a stream of `HashMap`s.
impl<K, V, U, S1, S2> FromIterator<HashMap<K, U, S1>> for HashMap<K, V, S2>
where
    K: Eq + Hash,
    S2: Default + BuildHasher,
    U: Into<V>,
    V: std::iter::Extend<U>,
{
    fn from_iter<Iter: IntoIterator<Item = HashMap<K, U, S1>>>(iter: Iter) -> Self {
        let mut m = Self::default();
        m.extend(iter);
        m
    }
}
