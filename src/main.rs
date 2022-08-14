#![feature(extend_one)]
#![feature(step_trait)]
#![feature(sync_unsafe_cell)]

mod expect_lazy;
mod extending;
mod indented;
mod input;
mod join;
mod partial_eq;
mod relation;

use crate::indented::{indented, indented_by};
use crate::input::Input;
use anyhow::{Context, Result};
use clap::Parser;
use itertools::{repeat_n, Itertools};
use lazy_static::lazy_static;
use std::ffi::OsStr;
use std::io::Write;
use std::process::exit;
use std::thread::available_parallelism;
use std::{env, io};

#[derive(Parser, Debug)]
pub struct Args {
    /// File path to read input from. This must be an actual file as it will be memory mapped.
    #[clap(name = "FILE")]
    input: std::path::PathBuf,

    /// Relations to join, in order.
    #[clap(name = "RELATION")]
    relations: Vec<String>,

    /// Number of bytes per chunk. `0` means use the page size which is probably `4096`. You can
    /// check `getpagesize` for the actual value.
    #[clap(short = 'c', long = "chunk-size", name = "BYTES", default_value = "0")]
    chunk_size: usize,

    /// Number of worker threads to spawn. `0` means to ask the system for a suitable value. Use
    /// `1` for sequential work.
    #[clap(short = 'j', long = "jobs", name = "JOBS", default_value = "0")]
    thread_count: usize,

    /// Print debug messages.
    #[clap(short, long)]
    debug: bool,

    /// List the contained relations/properties instead of joining.
    #[clap(short, long)]
    list_relations: bool,

    /// Display the entries of <TABLE>.
    #[clap(short = 't', long, name = "TABLE")]
    show_table: Vec<String>,

    /// Show the chunk division instead of joining.
    #[clap(long)]
    show_chunks: bool,

    /// Perform a hash join.
    #[clap(long = "hash")]
    hash_join: bool,

    /// Perform a sort-merge join.
    #[clap(long = "sort")]
    sort_merge_join: bool,

    /// Print the first N join results.
    #[clap(short, long = "print")]
    print_result: bool,

    /// The number of join results to print when enabled. Use ‘0’ to print everything.
    #[clap(short = 'n', long, name = "N", default_value = "10")]
    print_count: usize,

    /// Run the improved versions of the hash-join/sort-merge-join algorithms.
    #[clap(short, long)]
    improved: bool,
}

impl Args {
    fn adjust(&mut self) -> Result<bool> {
        let mut adjusted = false;

        if self.thread_count == 0 {
            self.thread_count = available_parallelism()?.into();
            adjusted = true;
        }

        Ok(adjusted)
    }

    fn chunk_count(&self) -> usize {
        if self.thread_count == 1 {
            1
        } else {
            self.thread_count + 1
        }
    }
}

lazy_static! {
    static ref USE_COLORS: bool = {
        let env_dumb = env::var_os("TERM") == Some(OsStr::new("dumb").into());
        let env_no_color = env::var_os("NO_COLOR").is_some();
        !env_dumb && !env_no_color && atty::is(atty::Stream::Stderr)
    };
    static ref DBG_PRFX: String = colored("1;2", "DBG: ");
}

fn colored(code: &str, msg: &str) -> String {
    if *USE_COLORS {
        format!("\x1b[{}m{}\x1b[0m", code, msg)
    } else {
        msg.to_owned()
    }
}

fn list_relations(input: &Input) -> Result<bool> {
    let unique_props = input.iter_lines().map(|ln| ln.parse().1).unique();
    let mut handle = io::stdout().lock();
    for prop in unique_props {
        writeln!(handle, "{}", prop)?;
    }
    Ok(true)
}

fn show_chunks(args: &Args, input: &Input) -> Result<bool> {
    let chunks = input.divide_chunks(args.chunk_count(), args.chunk_size);
    let mut handle = io::stdout().lock();
    let counts = if args.debug {
        chunks
            .into_iter()
            .enumerate()
            .map(|(i, chunk)| -> Result<usize> {
                writeln!(handle, "Chunk #{}:", i + 1)?;
                let mut n = 0;
                for ln in chunk {
                    n += 1;
                    writeln!(handle, "  {:?}", String::from_utf8_lossy(ln.data))?
                }
                Ok(n)
            })
            .collect::<Result<Vec<usize>>>()
            .and_then(|res| {
                writeln!(handle)?;
                Ok(res)
            })
    } else {
        Ok(chunks.into_iter().map(|chunk| chunk.count()).collect())
    }?;

    let n_sum: usize = counts.iter().sum();
    let max_width = n_sum.to_string().len();
    let chnk_width = format!("#{}", counts.len()).len();

    for (i, n) in counts.into_iter().enumerate() {
        let desc = format!("#{}", i + 1);
        writeln!(handle, " {:>chnk_width$}: {:>max_width$} lines", desc, n)?;
    }
    let div: String = repeat_n('-', chnk_width + max_width + 10).collect();
    let prfx: String = repeat_n(' ', chnk_width + 1).collect();
    writeln!(handle, "{}", div)?;
    writeln!(handle, "{}Σ {} lines", prfx, n_sum)?;
    Ok(true)
}

fn try_main() -> Result<bool> {
    let mut args = {
        if let Ok(env_args) = env::var("SPARQL_JOIN_ARGS") {
            eprintln!("warning: taking aditional arguments from SPARQL_JOIN_ARGS.");
            Args::parse_from(env::args_os().chain(env_args.split_ascii_whitespace().map_into()))
        } else {
            Args::parse()
        }
    };
    macro_rules! dbgln {
        () => {
            if args.debug { eprintln!("{}", *DBG_PRFX) }
        };
        ($($arg:tt)*) => {{
            if args.debug { eprintln!("{}", indented_by(format!($($arg)*), &*DBG_PRFX)) }
        }};
    }

    dbgln!("parsed arguments: {:#?}", args);
    dbgln!();
    if args.adjust()? {
        dbgln!("adjusted arguments: {:#?}", args);
        dbgln!();
    }

    let input = Input::open(&args.input)
        .with_context(|| format!("Cannot read file ‘{}’", args.input.display()))?;
    dbgln!("input opened: {:#?}", input);
    dbgln!();

    if args.list_relations {
        list_relations(&input)
    } else if args.show_chunks {
        show_chunks(&args, &input)
    } else {
        join::join(&args, &input)
    }
}

fn main() {
    let failed = match try_main() {
        Ok(good) => !good,
        Err(err) => {
            // Print "error:" in bold with red foreground.
            let prefix_msg = colored("1;31", "Error:");
            // Print "caused by:" with yellow foreground.
            let cause_msg = colored("33", "Caused by:");

            // Print the actual errors.
            //
            //     Error: <main reason>
            //
            //     Caused by:
            //          <A>
            //     Caused by:
            //          <B>
            //     ...
            eprintln!();
            eprintln!("{} {}", prefix_msg, err);
            for (i, cause) in err.chain().skip(1).enumerate() {
                if i == 0 {
                    eprintln!()
                }
                eprintln!("{}\n{}", cause_msg, indented(cause));
            }

            // Indicate failure.
            true
        }
    };

    if failed {
        exit(1)
    }
}
