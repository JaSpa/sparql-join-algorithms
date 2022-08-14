use crate::input;
use std::collections::HashMap;

pub type Relation = Vec<(input::Field, input::Field)>;
pub type StrRelation<'a> = Vec<(input::Str<'a>, input::Str<'a>)>;

pub type Universe<'a> = HashMap<input::Str<'a>, StrRelation<'a>>;

/*
#[derive(Debug, Default)]
pub struct Universe<'a>(HashMap<input::Str<'a>, Relation>);

impl<'a> Universe<'a> {
    pub fn get(&self, name: &str) -> Option<&Relation> {
        let short_field = input::Str::new(name);
        self.0.get(&short_field)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut Relation> {
        let short_field = input::Str::new(name);
        self.0.get_mut(&short_field)
    }
}
*/

/*
impl<'a> FromIterator<Triple<'a>> for Universe<'a> {
    fn from_iter<T: IntoIterator<Item = Triple<'a>>>(iter: T) -> Self {
        let mut u = Self::default();
        u.extend(iter);
        u
    }
}

impl<'a> FromIterator<Universe<'a>> for Universe<'a> {
    fn from_iter<T: IntoIterator<Item = Universe<'a>>>(iter: T) -> Self {
        let mut u = Self::default();
        u.extend(iter);
        u
    }
}

impl<'a> Extend<Triple<'a>> for Universe<'a> {
    fn extend<T: IntoIterator<Item = Triple<'a>>>(&mut self, iter: T) {
        for triple in iter {
            self.0
                .entry(triple.property)
                .or_default()
                .extend_one(triple)
        }
    }
}

impl<'a> Extend<Universe<'a>> for Universe<'a> {
    fn extend<T: IntoIterator<Item = Universe<'a>>>(&mut self, iter: T) {
        for other in iter {
            for (k, v) in other.0 {
                self.0.entry(k).or_default().extend_one(v)
            }
        }
    }
}

impl<'a> FromIterator<Triple<'a>> for Relation<'a> {
    fn from_iter<T: IntoIterator<Item = Triple<'a>>>(iter: T) -> Self {
        let mut r = Self::default();
        r.extend(iter);
        r
    }
}

impl<'a> Extend<Triple<'a>> for Relation<'a> {
    fn extend<T: IntoIterator<Item = Triple<'a>>>(&mut self, iter: T) {
        self.0.extend(iter.into_iter().map(|triple| Entry {
            subject: triple.subject,
            object: triple.object,
        }));
    }
}

impl<'a> Extend<Relation<'a>> for Relation<'a> {
    fn extend<T: IntoIterator<Item = Relation<'a>>>(&mut self, iter: T) {
        self.0.extend(iter.into_iter().flat_map(|r| r.0))
    }
}
*/
