use std::collections::HashSet;
use std::iter::zip;

use std::mem::ManuallyDrop;

use std::usize;
use std::{io, io::Write};

use anyhow::{bail, Result};
use itertools::Itertools;
use rayon::iter::{FromParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::input::{self, Field, Input};
use crate::relation::Relation;
use crate::{colored, Args};

mod pipeline;
use pipeline::Pipeline;

type RelSet<'a> = HashSet<input::Str<'a>>;

pub fn join(args: &Args, input: &Input) -> Result<bool> {
    let joining_rels = args
        .relations
        .iter()
        .map(|name| input::Str::new(name))
        .collect_vec();
    let rels_set: RelSet = joining_rels
        .iter()
        .copied()
        .chain(args.show_table.iter().map(|name| input::Str::new(name)))
        .collect();

    let universe = input
        .iter_lines()
        .map(|ln| ln.parse())
        .filter(|triple| rels_set.contains(&triple.1))
        .map(|triple| (triple.1, (triple.0, triple.2)))
        .into_group_map();

    // Print any requested relations.
    {
        let mut handle = io::stdout().lock();
        for name in args.show_table.iter() {
            writeln!(handle, "{}", colored("1", &format!("==== {} ====", name)))?;
            let rel = match universe.get(&input::Str::new(name)) {
                Some(r) => r,
                None => {
                    writeln!(handle, "{}\n", colored("3", "-- empty --"))?;
                    continue;
                }
            };

            let width_col1 = rel.iter().map(|e| e.0.len()).max().unwrap_or(0);
            for entry in rel {
                writeln!(handle, "{:width_col1$}\t{}", entry.0, entry.1)?;
            }
            writeln!(handle)?;
        }
    }

    if !args.hash_join && !args.sort_merge_join {
        bail!("Neither --hash nor --sort specified.")
    }
    if args.hash_join && args.sort_merge_join {
        bail!("Modes --hash and --sort are mutually exclusive.")
    }

    let settings = Settings {
        join_count: args.relations.len(),
    };

    let pipeline = Pipeline::build(input, &universe, &args.relations)?;
    let mut join_impl: ManuallyDrop<Box<dyn JoinAlgo>> = ManuallyDrop::new(if args.hash_join {
        Box::new(hash::Impl::new(args.improved))
    } else {
        Box::new(sort_merge::Impl::new(args.improved))
    });

    for (i, ((relation, name), range)) in pipeline
        .relations
        .into_iter()
        .zip(&args.relations)
        .zip(pipeline.ranges)
        .enumerate()
    {
        eprintln!();
        eprintln!("-- Joining {}", name);
        join_impl.join(&settings, i, relation, range);
        eprintln!("-- {} entries", join_impl.results().len());
    }

    let join_results = join_impl.results();
    let result_count = join_results.len();

    println!();
    if args.print_result {
        let print_count = if args.print_count > 0 {
            result_count.min(args.print_count)
        } else {
            result_count
        };

        // Decode all columns.
        let decoded = join_results
            .take(print_count)
            .enumerate()
            .map(|(i, fields)| {
                if fields.contains(&Field::INVALID) {
                    eprintln!("invalid field in row {}: {:?}", i, fields);
                }
                fields
                    .iter()
                    .map(|f| input.extract_str(*f).decode())
                    .collect_vec()
            })
            .collect_vec();

        // Count column widths.
        let Columns(widths) = decoded
            .par_iter()
            .map(|cols| Columns(cols.iter().map(|c| c.len().max(1)).collect_vec()))
            .collect();

        let write_div = |h: &mut io::StdoutLock, cbase: char, csplit: char| -> Result<()> {
            for (i, w) in widths.iter().copied().enumerate() {
                if i == 0 {
                    // The first column gets an extra `cbase`.
                    write!(h, "{}", cbase)?;
                }
                (0..w).try_for_each(|_| write!(h, "{}", cbase))?;
                if i + 1 == widths.len() {
                    // The last column gets an extra `cbase`.
                    write!(h, "{}", cbase)?
                } else {
                    // Columns in the middle get the divider.
                    write!(h, "{}{}{}", cbase, csplit, cbase)?
                }
            }
            writeln!(h)?;
            Ok(())
        };

        // Print everything.
        let mut io = io::stdout().lock();
        write_div(&mut io, '═', '╤')?;
        for (i, cols) in decoded.into_iter().enumerate() {
            if i % 5 == 0 && i > 0 {
                write_div(&mut io, '─', '┼')?;
            }
            for (i, (field, width)) in zip(&cols[..cols.len() - 1], &widths).enumerate() {
                if i == 0 {
                    write!(io, " ")?;
                }
                write!(io, "{field:width$} │ ")?;
            }
            if let Some(last_field) = cols.last() {
                write!(io, "{last_field}")?;
            }
            writeln!(io)?;
        }
        if print_count > 0 && print_count % 5 == 0 && print_count < result_count {
            write_div(&mut io, '─', '┼')?;
        }
        if print_count < result_count {
            for (i, w) in widths.iter().copied().enumerate() {
                if i == 0 {
                    write!(io, " ")?;
                } else {
                    write!(io, "│ ")?;
                }
                write!(io, "⋮{:w$}", "")?;
            }
            writeln!(io)?;
        }
        write_div(&mut io, '═', '╧')?;
    }

    println!("{} results", result_count);
    Ok(true)
}

#[derive(Default)]
struct Columns(Vec<usize>);

impl FromIterator<Columns> for Columns {
    fn from_iter<T: IntoIterator<Item = Columns>>(iter: T) -> Self {
        let mut cols = Columns(Vec::new());
        cols.extend(iter);
        cols
    }
}

impl Extend<Columns> for Columns {
    fn extend<T: IntoIterator<Item = Columns>>(&mut self, iter: T) {
        let widths = &mut self.0;
        for Columns(cs) in iter {
            if widths.len() < cs.len() {
                widths.resize(cs.len(), 0);
            }
            zip(widths.iter_mut(), cs).for_each(|(w, c)| *w = (*w).max(c))
        }
    }
}

impl FromParallelIterator<Columns> for Columns {
    fn from_par_iter<I>(par_iter: I) -> Self
    where
        I: rayon::iter::IntoParallelIterator<Item = Columns>,
    {
        par_iter
            .into_par_iter()
            .reduce(Columns::default, |Columns(mut c1), Columns(c2)| {
                if c1.is_empty() {
                    return Columns(c2);
                }
                if c2.is_empty() {
                    return Columns(c1);
                }
                c1.resize(c2.len(), 0);
                zip(c1.iter_mut(), c2).for_each(|(w, c)| *w = (*w).max(c));
                Columns(c1)
            })
    }
}

struct Settings {
    pub join_count: usize,
}

trait JoinAlgo {
    fn join(
        &mut self,
        settings: &Settings,
        index: usize,
        relation: Relation,
        field_range: (Field, Field),
    );
    fn results<'a>(&'a self) -> Box<dyn ExactSizeIterator<Item = &'a Vec<Field>> + 'a>;
}

mod hash {

    use std::{collections::HashMap, mem, ops::Range};

    use rayon::iter::*;

    use crate::input::Field;

    use super::*;

    pub struct Impl {
        improved: bool,
        join_table: Vec<Vec<Field>>,
        field_ranges: [Range<Field>; 8],
        hash_tables: [HashMap<Field, Vec<Vec<Field>>>; 8],
    }

    impl Impl {
        pub fn new(improved: bool) -> Self {
            Impl {
                improved,
                join_table: Vec::new(),
                field_ranges: [
                    Field::INVALID..Field::INVALID,
                    Field::INVALID..Field::INVALID,
                    Field::INVALID..Field::INVALID,
                    Field::INVALID..Field::INVALID,
                    Field::INVALID..Field::INVALID,
                    Field::INVALID..Field::INVALID,
                    Field::INVALID..Field::INVALID,
                    Field::INVALID..Field::INVALID,
                ],
                hash_tables: [
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                ],
            }
        }

        /// Hashes `self.join_table` into `self.hash_tables[0]`. `self.field_ranges[0]` is
        /// adjusted to include the whole set of ranges.
        fn simple_hash(&mut self, index: usize) {
            eprintln!(
                "++ Hashing left hand side ({} entries)",
                self.join_table.len()
            );

            while let Some(fields) = self.join_table.pop() {
                self.hash_tables[0]
                    .entry(fields[index])
                    .or_default()
                    .push(fields)
            }

            self.field_ranges.fill(Field::INVALID..Field::INVALID);
            self.field_ranges[0] = Field::from_offset(0).make_range(usize::MAX);
        }

        /// Hashes `self.join_table` into the full width of `self.hash_tables`.
        /// `self.field_ranges` is adjusted to reflect the partitioning.
        fn partitioned_hash(&mut self, index: usize, field_range: (Field, Field)) {
            debug_assert!(
                field_range.0 <= field_range.1,
                "invalid range: {:?}",
                field_range
            );
            debug_assert!(field_range.0 != Field::INVALID);

            eprintln!(
                "++ Hashing left hand side ({} entries)",
                self.join_table.len()
            );
            let full_range = field_range.0.offset()..field_range.1.offset() + 1;
            let per_chunk = usize::max(full_range.len() / 8, 128);

            // Distribute the ranges.
            self.field_ranges.fill(Field::INVALID..Field::INVALID);
            self.field_ranges[0] = Field::from_offset(0).make_range(full_range.start + per_chunk);

            for i in 1..self.field_ranges.len() {
                self.field_ranges[i] = self.field_ranges[i - 1].end.make_range(per_chunk);
            }

            // For each range hash the correct set of elements from join_table.
            self.field_ranges
                .par_iter()
                .zip(self.hash_tables.par_iter_mut())
                .for_each(|(range, table)| {
                    for fields in &self.join_table {
                        if range.contains(&fields[index]) {
                            table.entry(fields[index]).or_default().push(fields.clone());
                        }
                    }
                });
        }

        fn scan_hashed(&mut self, index: usize, relation: Relation) {
            // Clear out the old join table (which now exists in hashed form) in parallel.
            eprintln!("++ Clearing out join table",);
            mem::take(&mut self.join_table)
                .into_par_iter()
                .for_each(mem::drop);

            eprintln!(
                "++ Scanning through right hand side ({} entries)",
                relation.len()
            );
            self.join_table = relation
                .into_par_iter()
                .flat_map_iter(|(subj, obj)| {
                    // Find the correct index. Although partition_point can return an index equal
                    // to the slice length we know that all values inside the relation are
                    // included in the range based on how they are built in Pipeline::build.
                    let idx = self.field_ranges.partition_point(|r| r.start <= subj);
                    let hm = &self.hash_tables[idx - 1];
                    hm.get(&subj).into_iter().flat_map(move |field_list| {
                        field_list.iter().cloned().map(move |mut fields| {
                            fields[index + 1] = obj;
                            fields
                        })
                    })
                })
                .collect();
        }
    }

    impl JoinAlgo for Impl {
        fn join(
            &mut self,
            settings: &Settings,
            index: usize,
            relation: Relation,
            field_range: (Field, Field),
        ) {
            if index == 0 {
                self.join_table
                    .extend(relation.into_iter().map(|(subj, obj)| {
                        let mut v = vec![Field::INVALID; settings.join_count + 1];
                        v[0] = subj;
                        v[1] = obj;
                        v
                    }));
                return;
            }

            eprintln!("++ Clearing out hash tables.");
            self.hash_tables
                .par_iter_mut()
                .for_each(|table| table.clear());

            if self.improved {
                self.partitioned_hash(index, field_range);
            } else {
                self.simple_hash(index);
            }

            self.scan_hashed(index, relation)
        }

        fn results<'a>(&'a self) -> Box<dyn ExactSizeIterator<Item = &'a Vec<Field>> + 'a> {
            Box::new(self.join_table.iter())
        }
    }
}

mod sort_merge {

    use std::{mem, sync::mpsc::channel};

    use rayon::{
        iter::{IndexedParallelIterator, ParallelIterator},
        slice::ParallelSliceMut,
    };

    use super::*;

    pub struct Impl {
        improved: bool,
        join_table: Vec<Vec<Field>>,
        del_buffer: Vec<(usize, Vec<usize>)>,
    }

    impl Impl {
        pub fn new(improved: bool) -> Self {
            Impl {
                improved,
                join_table: Default::default(),
                del_buffer: Default::default(),
            }
        }
    }

    impl JoinAlgo for Impl {
        fn join(
            &mut self,
            settings: &Settings,
            index: usize,
            mut relation: Relation,
            _field_range: (Field, Field),
        ) {
            if index == 0 {
                self.join_table
                    .extend(relation.into_iter().map(|(subj, obj)| {
                        let mut fields = vec![Field::INVALID; settings.join_count + 1];
                        fields[0] = subj;
                        fields[1] = obj;
                        fields
                    }));
                return;
            }

            let jt_key = |fields: &Vec<Field>| fields[index];
            if self.improved {
                eprintln!(
                    "++ [sorting-par]  left-hand side: {} entries",
                    self.join_table.len()
                );
                self.join_table.par_sort_unstable_by_key(jt_key);
                eprintln!(
                    "++ [sorting-par] right-hand side: {} entries",
                    relation.len()
                );
                relation.par_sort_unstable();
            } else {
                eprintln!("++ [sorting-seq]");
                eprintln!("++  left-hand side: {} entries", self.join_table.len());
                eprintln!("++ right-hand side: {} entries", relation.len());
                rayon::join(
                    || self.join_table.sort_unstable_by_key(jt_key),
                    || relation.sort_unstable(),
                );
            }

            // Join the tables.
            //
            // We split the left hand side into chunks determine the starting point in the right
            // hand side for each chunk and directly modify `self.join_table` with the results.
            //
            // If rows have to be duplicated we send them via a channel to be appended later. If
            // rows have to be removed we send the index to be removed.
            eprintln!("++ merging tables");
            let (dup_send, dup_recv) = channel::<Vec<Field>>();
            let chunk_size = 1024;
            self.join_table
                .par_chunks_mut(chunk_size)
                .enumerate()
                .map_with(dup_send, |dup, (chunk_index, chunk)| {
                    let fst_key = chunk.first().unwrap()[index];
                    let mut i = relation.partition_point(|x| x.0 < fst_key);

                    let chunk_base = chunk_index * chunk_size;
                    let chunk_len = chunk.len();
                    let abort = |idx| {
                        (
                            chunk_base,
                            Vec::from_iter(chunk_base + idx..chunk_base + chunk_len),
                        )
                    };

                    if i >= relation.len() {
                        return abort(0);
                    }

                    let mut del_indices = Vec::new();
                    for (r_idx, row) in chunk.iter_mut().enumerate() {
                        let lhs_k = row[index];

                        // If the right hand side is smaller, advance.
                        while relation[i].0 < lhs_k {
                            i += 1;

                            if i >= relation.len() {
                                return abort(r_idx);
                            }
                        }

                        if relation[i].0 != lhs_k {
                            // Remove this row if there is no matching entry.
                            del_indices.push(chunk_base + r_idx);
                            continue;
                        }

                        // Update this row in-place.
                        debug_assert!(relation[i].1.is_valid());
                        row[index + 1] = relation[i].1;

                        // Maybe we have to insert additional rows.
                        for entry in relation[i + 1..].iter().take_while(|x| x.0 == lhs_k) {
                            debug_assert!(entry.1.is_valid());
                            let mut new_row = row.clone();
                            new_row[index + 1] = entry.1;
                            dup.send(new_row).unwrap();
                        }
                    }

                    (chunk_index, del_indices)
                })
                .collect_into_vec(&mut self.del_buffer);

            // Make sure we start removing from the end.
            eprintln!("++ deleting excessive rows");
            self.del_buffer.sort_unstable();
            for idx in mem::take(&mut self.del_buffer)
                .into_iter()
                .flat_map(|(_, indices)| indices.into_iter())
                .rev()
            {
                self.join_table.swap_remove(idx);
            }

            // Insert all the new rows.
            eprintln!("++ inserting additional rows");
            self.join_table.extend(dup_recv);
        }

        fn results<'a>(&'a self) -> Box<dyn ExactSizeIterator<Item = &'a Vec<Field>> + 'a> {
            Box::new(self.join_table.iter())
        }
    }
}
