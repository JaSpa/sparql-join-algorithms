use std::collections;
use std::ops::{Deref, DerefMut};

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct LinkedList<T>(pub collections::LinkedList<T>);

impl<T> Default for LinkedList<T> {
    fn default() -> Self {
        collections::LinkedList::default().into()
    }
}

impl<T> From<LinkedList<T>> for collections::LinkedList<T> {
    fn from(ll: LinkedList<T>) -> Self {
        ll.0
    }
}

impl<T> From<collections::LinkedList<T>> for LinkedList<T> {
    fn from(ll: collections::LinkedList<T>) -> Self {
        LinkedList(ll)
    }
}

impl<T> Deref for LinkedList<T> {
    type Target = collections::LinkedList<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for LinkedList<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Creating a `LinkedList` from a single element.
impl<T> From<T> for LinkedList<T> {
    fn from(elem: T) -> Self {
        collections::LinkedList::from([elem]).into()
    }
}

/// Creating a `LinkedList` from a fixed array.
impl<T, const N: usize> From<[T; N]> for LinkedList<T> {
    fn from(elems: [T; N]) -> Self {
        collections::LinkedList::from(elems).into()
    }
}

/// Extending a `LinkedList<T>` from a stream of `T`s.
impl<T> Extend<T> for LinkedList<T> {
    fn extend<Iter: IntoIterator<Item = T>>(&mut self, iter: Iter) {
        self.0.extend(iter)
    }
}

/// Creating a `LinkedList<T>` from a stream of `T`s.
impl<T> FromIterator<T> for LinkedList<T> {
    fn from_iter<Iter: IntoIterator<Item = T>>(iter: Iter) -> Self {
        collections::LinkedList::from_iter(iter).into()
    }
}

/// Extending a `LinkedList` from a stream of `LinkedList`s.
impl<T> Extend<LinkedList<T>> for LinkedList<T> {
    fn extend<Iter: IntoIterator<Item = LinkedList<T>>>(&mut self, iter: Iter) {
        for mut ll in iter {
            self.0.append(&mut ll.0)
        }
    }
}

/// Creating a `LinkedList` from a stream of `LinkedList`s.
impl<T> FromIterator<LinkedList<T>> for LinkedList<T> {
    fn from_iter<Iter: IntoIterator<Item = LinkedList<T>>>(iter: Iter) -> Self {
        let mut ll = Self::default();
        ll.extend(iter);
        ll
    }
}
