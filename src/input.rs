use anyhow::Result;
use libc::c_int;
use memchr::{memchr, memchr2, memchr_iter, memrchr};
use memmap::Mmap;
use std::borrow::Cow;

use std::fmt;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io::Error;
use std::ops::Range;
use std::path::PathBuf;

#[derive(Debug)]
pub struct Input {
    pub path: PathBuf,
    data: Mmap,
}

impl Input {
    pub fn open(path: &PathBuf) -> Result<Input, Error> {
        let file = OpenOptions::new().read(true).open(&path)?;
        let data = unsafe { Mmap::map(&file)? };
        Ok(Input {
            path: path.clone(),
            data,
        })
    }

    fn mk_chunk_iter<'a>(
        chunk: &'a [u8],
        offset: usize,
        skip_first: bool,
    ) -> Box<dyn Iterator<Item = InputLine<'a>> + Send + 'a> {
        Box::new(
            memchr_iter(ascii::nl(), chunk)
                .scan(0, move |start_idx, nl_idx| {
                    let range = *start_idx..=nl_idx;
                    let line = InputLine {
                        offset: offset + range.start(),
                        data: &chunk[range],
                    };
                    *start_idx = nl_idx + 1;
                    Some(line)
                })
                .skip(if skip_first { 1 } else { 0 }),
        )
    }

    pub fn iter_lines<'a>(&'a self) -> Box<dyn Iterator<Item = InputLine<'a>> + 'a> {
        Self::mk_chunk_iter(&self.data, 0, false)
    }

    pub fn divide_chunks<'a>(
        &'a self,
        count: usize,
        size_hint: usize,
    ) -> Vec<Box<dyn Iterator<Item = InputLine<'a>> + Send + 'a>> {
        if count < 3 {
            // If we want to split into one or two working operations we can't divide the work
            // because we need at least one worker to handle the entries crossing the pages.
            return vec![Box::new(Self::mk_chunk_iter(&self.data, 0, false))];
        }

        let page_size = if size_hint == 0 {
            unsafe { getpagesize() as usize }
        } else {
            size_hint
        };

        // Create a vector of the iterators to traverse the data. We know exactly how many
        // iterators there are (at most).
        let mut iters = Vec::<Box<dyn Iterator<Item = _> + Send>>::new();
        iters.reserve_exact(count);

        // We have `count - 1` workers since the count-th one is responsible of iterating the
        // lines spanning chunk breaks.
        let workers = count - 1;

        // Just distribute chunks of size page_size across all workers. Some may end up empty.
        let chunk_size = if workers * page_size >= self.data.len() {
            // We do not use a chunk size smaller than the page size if this would utilise more
            // workers.
            page_size
        } else {
            best_chunks(workers, page_size, self.data.len())
        };

        // Add all the chunk iterators.
        iters.extend(
            self.data
                .chunks(chunk_size)
                .enumerate()
                // If this is not the first chunk we have to skip over the first "line".
                .map(|(i, c)| Self::mk_chunk_iter(c, i * chunk_size, i > 0)),
        );

        // Push the final iterator which handles all the boundary crossing lines.
        iters.push(Box::new(BreakChunk {
            full_buffer: &self.data,
            base_offset: 0,
            chunk_size,
        }));

        iters
    }

    pub fn extract_str(&self, field: Field) -> Str {
        assert!(field.is_valid());
        let remaining = &self.data[field.0..];
        Str(&remaining[..field_len(remaining)])
    }

    pub fn extract_field(&self, s: Str) -> Field {
        let data_range = self.data.as_ptr_range();
        let s_range = s.0.as_ptr_range();
        assert!(data_range.start <= s_range.start && s_range.end <= data_range.end);
        Field(unsafe { s_range.start.offset_from(data_range.start) as usize })
    }
}

fn best_chunks(count: usize, base: usize, length: usize) -> usize {
    // The minimum amount of work per worker.
    let min_per_w = length / count;
    let base_2 = base.next_power_of_two();
    // Round `min_per_w` up to the next multiple of `base_2`. Since we know that it is a power of
    // two we can this faster calculation than the general method involving taking the remainder.
    (min_per_w + base_2 - 1) & (!base_2 + 1)
}

pub struct BreakChunk<'a> {
    full_buffer: &'a [u8],
    base_offset: usize,
    chunk_size: usize,
}

impl<'a> Iterator for BreakChunk<'a> {
    type Item = InputLine<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.full_buffer.len() < self.chunk_size {
            return None;
        }

        // xxNx|xNxx
        //    ---´
        //
        // xxxN|xxxN|
        //      ---´
        //
        let (prev, rest) = self.full_buffer.split_at(self.chunk_size);
        let start = memrchr(ascii::nl(), prev)? + 1;
        let end = memchr(ascii::nl(), rest)? + self.chunk_size;
        let broken_line = &self.full_buffer[start..=end];
        let offset = self.base_offset + start;
        self.full_buffer = rest;
        self.base_offset += prev.len();
        Some(InputLine {
            offset,
            data: broken_line,
        })
    }
}

#[link(name = "c")]
extern "C" {
    fn getpagesize() -> c_int;
}

fn field_len(data: &[u8]) -> usize {
    let c = *data.first().expect("empty data");
    if c == ascii::dquote() {
        // Skip until the next double quote. Include that quote in the field.
        memchr(ascii::dquote(), &data[1..]).expect("missing closing DQUOTE") + 2
    } else {
        // Skip until the next tab or space character.
        memchr2(ascii::tab(), ascii::space(), data).expect("missing terminating TAB or SPACE")
    }
}

mod ascii {
    macro_rules! char {
        ($name:ident, $c:expr) => {
            pub fn $name() -> u8 {
                $c.try_into().unwrap()
            }
        };
    }

    char!(nl, '\n');
    char!(tab, '\t');
    char!(space, ' ');
    char!(dquote, '"');
}

#[derive(Debug, Copy, Clone)]
pub struct InputLine<'a> {
    pub offset: usize,
    pub data: &'a [u8],
}

impl<'a> InputLine<'a> {
    pub fn parse(self) -> (Str<'a>, Str<'a>, Str<'a>) {
        let subj = field_len(self.data);
        let prop = field_len(&self.data[subj + 1..]); // Add +1 to skip over the separating TAB
        let objf = field_len(&self.data[subj + 1 + prop + 1..]);
        (
            Str(&self.data[..subj]),
            Str(&self.data[subj + 1..][..prop]),
            Str(&self.data[subj + 1 + prop + 1..][..objf]),
        )
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Field(usize);

impl Field {
    pub const INVALID: Field = Field(usize::MAX);

    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }

    pub fn offset(self) -> usize {
        self.0
    }

    pub fn advance(self, by: usize) -> Self {
        Field(self.0.checked_add(by).unwrap_or(usize::MAX))
    }

    pub fn from_offset(off: usize) -> Self {
        Field(off)
    }

    pub fn make_range(self, length: usize) -> Range<Self> {
        self..self.advance(length)
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Str<'a>(&'a [u8]);

impl<'a> Str<'a> {
    pub fn new(string: &'a str) -> Self {
        Str(string.as_bytes())
    }

    pub fn decode(self) -> Cow<'a, str> {
        String::from_utf8_lossy(self.0)
    }

    pub fn len(self) -> usize {
        self.0.len()
    }
}

impl fmt::Display for Str<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.decode().fmt(f)
    }
}
