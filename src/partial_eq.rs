use core::hash::Hash;
use std::cmp;

#[derive(Debug, Clone, Copy)]
pub struct PartialEq<T>(pub Option<T>);

impl<T: Hash> Hash for PartialEq<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl<T: cmp::PartialEq> cmp::PartialEq for PartialEq<T> {
    fn eq(&self, other: &Self) -> bool {
        if let Some(ref a) = self.0 {
            if let Some(ref b) = other.0 {
                return a == b;
            }
        }
        false
    }
}

impl<T: Eq> Eq for PartialEq<T> {}
