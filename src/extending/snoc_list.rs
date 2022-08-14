use std::{mem, num::NonZeroUsize, ptr, sync::Arc};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonEmpty<T>(Arc<Node<T>>);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Node<T> {
    size: NonZeroUsize,
    value: T,
    rest: Option<NonEmpty<T>>,
}

impl<T> NonEmpty<T> {
    pub fn new(value: T) -> Self {
        Arc::new(Node {
            value,
            rest: None,
            size: nonzero_lit::usize!(1),
        })
        .into()
    }

    pub fn snoc(self, value: T) -> Self {
        let size = unsafe { NonZeroUsize::new_unchecked(self.0.size.get() + 1) };
        Arc::new(Node {
            size,
            value,
            rest: Some(self),
        })
        .into()
    }

    pub fn snocs(self, values: impl IntoIterator<Item = T>) -> Self {
        values.into_iter().fold(self, Self::snoc)
    }

    pub fn last(&self) -> &T {
        &self.0.value
    }

    pub fn len(&self) -> NonZeroUsize {
        self.0.size
    }

    pub fn map_vec<U>(&self, mut transform: impl FnMut(&T) -> U) -> Vec<U> {
        let mut v = Vec::with_capacity(self.len().get());
        let v_ptr: *mut U = v.as_mut_ptr();

        // Fill `v` from the back. This is not really unwind safe: If `transform` panics at some
        // point the already transformed elements will be leaked. We probably want to increase
        // `v`s length after every write.
        unsafe {
            let mut ts = Some(self);
            while let Some(tts) = ts {
                ptr::write(v_ptr.add(tts.len().get() - 1), transform(&tts.0.value));
                ts = tts.0.rest.as_ref();
            }
            v.set_len(v.capacity())
        }

        v
    }
}

impl<T> From<Arc<Node<T>>> for NonEmpty<T> {
    fn from(inner: Arc<Node<T>>) -> Self {
        NonEmpty(inner)
    }
}

impl<T> Clone for NonEmpty<T> {
    fn clone(&self) -> Self {
        self.0.clone().into()
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnocList<T>(Option<NonEmpty<T>>);

impl<T> SnocList<T> {
    pub fn new() -> Self {
        SnocList(None)
    }

    pub fn single(value: T) -> Self {
        SnocList(Some(NonEmpty::new(value)))
    }

    /*
    pub fn len(&self) -> usize {
        self.0.as_ref().map_or(0, |ts| ts.len().get())
    }
    */

    pub fn snoc(self, value: T) -> Self {
        match self.0 {
            Some(ts) => ts.snoc(value).into(),
            None => Self::single(value),
        }
    }

    pub fn snocs(self, values: impl IntoIterator<Item = T>) -> Self {
        // Append the values to the existing non empty items.
        if let Some(ts) = self.0 {
            return ts.snocs(values).into();
        }

        // We are currently an empty list. If we have a first element we are guaranteed a
        // non-empty result.
        let mut iter = values.into_iter();
        if let Some(first) = iter.next() {
            return NonEmpty::new(first).snocs(iter).into();
        }

        // Otherwise the result is empty.
        Self::new()
    }

    pub fn map_vec<U>(&self, transform: impl FnMut(&T) -> U) -> Vec<U> {
        self.0
            .as_ref()
            .map_or(Vec::new(), |ts| ts.map_vec(transform))
    }
}

impl<T> Default for SnocList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for SnocList<T> {
    fn clone(&self) -> Self {
        SnocList(self.0.clone())
    }
}

impl<T> From<NonEmpty<T>> for SnocList<T> {
    fn from(ts: NonEmpty<T>) -> Self {
        SnocList(ts.into())
    }
}

impl<T> FromIterator<T> for SnocList<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::new().snocs(iter)
    }
}

impl<T> Extend<T> for SnocList<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        *self = mem::take(self).snocs(iter);
    }
}
