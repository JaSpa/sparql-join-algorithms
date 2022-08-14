use std::{cell::Cell, collections::HashMap, fmt, iter, mem};

use anyhow::bail;
use itertools::Itertools;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    input::{self, Field, Input},
    relation::{Relation, StrRelation, Universe},
};

pub struct Pipeline {
    pub relations: Vec<Relation>,
    pub ranges: Vec<(Field, Field)>,
}

impl Pipeline {
    pub fn build<'a>(
        input: &'a Input,
        universe: &Universe<'a>,
        relation_names: &[String],
    ) -> anyhow::Result<Self> {
        // Resolve relation names or collect all unknown names before aborting.
        let rels_or_errs: Validation<Vec<&StrRelation>, Vec<&String>> = relation_names
            .iter()
            .map(|name| match universe.get(&input::Str::new(name)) {
                Some(r) => Validation::Valid(r),
                None => Validation::Invalid(name),
            })
            .collect();
        let rels = match rels_or_errs {
            Validation::Valid(rels) => rels,
            Validation::Invalid(unknown) => {
                let n = unknown.len();
                bail!(
                    "unknown {}: {}",
                    if n == 1 { "relation" } else { "relations" },
                    unknown
                        .into_iter()
                        .enumerate()
                        .flat_map(|(i, s)| if i == 0 {
                            ["", s]
                        } else if i + 1 == n {
                            [", and ", s]
                        } else {
                            [", ", s]
                        })
                        .collect_display()
                )
            }
        };

        if rels.len() < 2 {
            bail!("no join to be performed");
        }

        // For each relation (except the last) build a map from properties to offset fields.
        let mut mapped_objs = Vec::new();
        rels[..rels.len() - 1]
            .par_iter()
            .map(|rel_ref| {
                let mut map = HashMap::new();
                let field_rel = rel_ref
                    .iter()
                    .map(|(subj, obj)| {
                        let obj_field = map
                            .entry(*obj)
                            .or_insert_with(|| input.extract_field(*subj));
                        (*subj, *obj_field)
                    })
                    .collect_vec();
                (field_rel, map)
            })
            .collect_into_vec(&mut mapped_objs);

        let mut mapped_rels = vec![Relation::default(); rels.len()];
        let mut field_ranges = vec![(Field::INVALID, Field::INVALID); rels.len()];
        let mut out_rels = mapped_rels.iter_mut();
        let mut out_ranges = field_ranges.iter_mut();

        rayon::in_place_scope(|scope| {
            let mut iter = mapped_objs.into_iter();
            let (initial_table, initial_dict) = iter.next().expect("mapped_objs too short");

            // Translate the first column into fields. Skip over the first out_range.
            _ = out_ranges.next().expect("field_ranges too short");
            let out_fst = out_rels.next().expect("mapped_rels too short");
            scope.spawn(|_| {
                *out_fst = initial_table
                    .into_iter()
                    .map(|(subj, obj_f)| (input.extract_field(subj), obj_f))
                    .collect_vec();
            });

            // Grab a reference to the last slots so that it will not get consumed by the `zip`
            // below.
            let out_last_rel = out_rels.next_back().expect("mapped_rels too short");
            let out_last_range = out_ranges.next_back().expect("field_ranges too short");

            // Align the middle tables with the preceeding dictionary to resolve the subject
            // columns.
            let mut current_dict = initial_dict;
            let zipped = iter.zip_longest(out_rels).zip_longest(out_ranges);
            for z in zipped {
                let (z, range_out) = z.both().expect("vectors too short");
                let ((table, next_dict), rel_out) = z.both().expect("vectors too short");

                let this_dict = mem::replace(&mut current_dict, next_dict);
                scope.spawn(move |_| {
                    Self::resolve(rel_out, range_out, this_dict, table.into_iter());
                });
            }

            // Resolve the last table which is not included in mapped_objs.
            scope.spawn(move |_| {
                let iter = rels
                    .last()
                    .expect("`rels.len() > 2` ensured above")
                    .iter()
                    .map(|&(subj, obj)| (subj, input.extract_field(obj)));
                Self::resolve(out_last_rel, out_last_range, current_dict, iter);
            });
        });

        Ok(Pipeline {
            relations: mapped_rels,
            ranges: field_ranges,
        })
    }

    fn resolve<'a>(
        rel_out: &mut Relation,
        range_out: &mut (Field, Field),
        dictionary: HashMap<input::Str<'a>, Field>,
        iter: impl IntoIterator<Item = (input::Str<'a>, Field)> + ExactSizeIterator,
    ) {
        rel_out.reserve(iter.len());

        let mut first = true;
        for (subj, obj_f) in iter {
            let subj_f = if let Some(&subj_f) = dictionary.get(&subj) {
                rel_out.push((subj_f, obj_f));
                subj_f
            } else {
                continue;
            };

            if first {
                *range_out = (subj_f, subj_f);
                first = false;
            } else {
                range_out.0 = range_out.0.min(subj_f);
                range_out.1 = range_out.1.max(subj_f);
            }
        }
    }
}

enum Validation<T, E> {
    Valid(T),
    Invalid(E),
}

impl<T, E> Validation<T, E> {
    fn invalid(self) -> Option<E> {
        if let Validation::Invalid(e) = self {
            Some(e)
        } else {
            None
        }
    }
}

impl<T: Default, E> Default for Validation<T, E> {
    fn default() -> Self {
        Self::Valid(Default::default())
    }
}

impl<T, U, E1, E2> FromIterator<Validation<U, E2>> for Validation<T, E1>
where
    T: Extend<U> + Default,
    E1: Extend<E2> + Default,
{
    fn from_iter<Iter: IntoIterator<Item = Validation<U, E2>>>(iter: Iter) -> Self {
        let mut v = Self::default();
        v.extend(iter);
        v
    }
}

impl<T, U, E1, E2> Extend<Validation<U, E2>> for Validation<T, E1>
where
    T: Extend<U> + Default,
    E1: Extend<E2> + Default,
{
    fn extend<Iter: IntoIterator<Item = Validation<U, E2>>>(&mut self, iter: Iter) {
        fn extend_valid<T: Extend<U>, U, E2>(
            valid: &mut T,
            iter: &mut impl Iterator<Item = Validation<U, E2>>,
        ) -> Result<(), E2> {
            struct ExtendValid<'a, T, E> {
                inner: &'a mut T,
                discoverd_error: Option<E>,
            }

            impl<T, U, E> Iterator for ExtendValid<'_, T, E>
            where
                T: Iterator<Item = Validation<U, E>>,
            {
                type Item = U;

                /// Returns the next valid value from `inner` or terminates with `None` if `inner`
                /// is either exhausted or an error value was encounterd.
                ///
                /// This iterator does not implement `FusedIterator` to save on some (mostly
                /// unnecessary) checks. Therefore it is not safe to continue calling `next` once
                /// this iterator has returned `None`.
                fn next(&mut self) -> Option<Self::Item> {
                    match self.inner.next()? {
                        Validation::Valid(u) => Some(u),
                        Validation::Invalid(e) => {
                            self.discoverd_error = Some(e);
                            None
                        }
                    }
                }

                fn size_hint(&self) -> (usize, Option<usize>) {
                    (0, self.inner.size_hint().1)
                }
            }

            let mut iter: ExtendValid<_, E2> = ExtendValid {
                inner: iter,
                discoverd_error: None,
            };
            valid.extend(&mut iter);
            if let Some(err) = iter.discoverd_error {
                Err(err)
            } else {
                Ok(())
            }
        }

        fn extend_invalid<U, E1: Extend<E2>, E2>(
            invalid: &mut E1,
            iter: impl Iterator<Item = Validation<U, E2>>,
        ) {
            invalid.extend(iter.filter_map(|v| v.invalid()))
        }

        let mut iter = iter.into_iter();

        match self {
            Validation::Valid(t) => {
                if let Err(e) = extend_valid(t, &mut iter) {
                    let mut err = E1::default();
                    err.extend(iter::once(e));
                    extend_invalid(&mut err, iter);
                    *self = Validation::Invalid(err);
                }
            }
            Validation::Invalid(e) => extend_invalid(e, iter),
        };
    }
}

struct DisplayAll<I>(Cell<Option<I>>);

impl<I> fmt::Display for DisplayAll<I>
where
    I: IntoIterator,
    I::Item: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for item in self.0.take().expect("multi-display of DisplayAll") {
            item.fmt(f)?
        }
        Ok(())
    }
}

trait AsDisplayAll: IntoIterator
where
    Self: Sized,
    Self::Item: fmt::Display,
{
    fn collect_display(self) -> DisplayAll<Self> {
        DisplayAll(Cell::new(Some(self)))
    }
}

impl<I: IntoIterator> AsDisplayAll for I
where
    I: Sized,
    I::Item: fmt::Display,
{
}
