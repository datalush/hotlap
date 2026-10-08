//! Hotlap REPL binary: parse flags, open a session and run the loop.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::io::{self, IsTerminal};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use hotlap::state::{DurableStateBackend, StateBackend};
use hotlap_cli::bootstrap::{BootstrapSinkFactory, BootstrapSourceFactory};
use hotlap_cli::repl;
use hotlap_runtime::{Session, SessionConfig};

/// Checkpoints kept when the CLI enables periodic checkpointing.
const RETAIN: usize = 3;
/// Interval used when only `--state-dir` is given.
const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);

/// Command-line settings for the REPL.
struct Args {
    state_dir: Option<PathBuf>,
    bootstrap: Option<String>,
    checkpoint_interval: Option<Duration>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("hotlap: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = Args::parse(env::args().skip(1))?;
    let mut session = open_session(&args)?;
    let stdin = io::stdin();
    let prompt = stdin.is_terminal();
    let mut stdout = io::stdout();
    repl::run(&mut session, stdin.lock(), &mut stdout, prompt)?;
    session.shutdown()?;
    Ok(())
}

impl Args {
    /// Parse `args`, rejecting unknown flags and missing values.
    fn parse(mut args: impl Iterator<Item = String>) -> io::Result<Self> {
        let mut parsed = Self {
            state_dir: None,
            bootstrap: None,
            checkpoint_interval: None,
        };
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--state-dir" => parsed.state_dir = Some(PathBuf::from(value(&mut args, &flag)?)),
                "--bootstrap" => parsed.bootstrap = Some(value(&mut args, &flag)?),
                "--checkpoint-interval" => {
                    let raw = value(&mut args, &flag)?;
                    let secs: u64 = raw.parse().map_err(|_| invalid(&raw))?;
                    parsed.checkpoint_interval = Some(Duration::from_secs(secs));
                }
                "-h" | "--help" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => return Err(invalid(other)),
            }
        }
        Ok(parsed)
    }
}

/// Open a [`Session`] configured from the parsed flags.
fn open_session(args: &Args) -> Result<Session, Box<dyn Error>> {
    let mut config = SessionConfig::new();
    if let Some(bootstrap) = &args.bootstrap {
        config = config
            .with_source_factory(Arc::new(BootstrapSourceFactory::new(bootstrap)))
            .with_sink_factory(Arc::new(BootstrapSinkFactory::new(bootstrap)));
    }
    let interval = args
        .checkpoint_interval
        .or_else(|| args.state_dir.as_ref().map(|_| DEFAULT_INTERVAL));
    if let Some(interval) = interval {
        config = config.with_checkpoint(interval, RETAIN, checkpoint_backend(args)?);
    }
    Ok(Session::open(config)?)
}

/// Backing store for checkpoints: the state directory, or memory without one.
fn checkpoint_backend(args: &Args) -> Result<Box<dyn StateBackend + Send>, io::Error> {
    match &args.state_dir {
        Some(dir) => Ok(Box::new(DurableStateBackend::open(dir)?)),
        None => Ok(Box::new(BTreeMap::<Vec<u8>, Vec<u8>>::new())),
    }
}

/// Consume the value following `flag`, or fail naming the flag.
fn value(args: &mut impl Iterator<Item = String>, flag: &str) -> io::Result<String> {
    args.next().ok_or_else(|| invalid(flag))
}

/// Build the error reported for an unknown flag or invalid value.
fn invalid(input: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("unknown or invalid argument: {input}"),
    )
}

fn print_usage() {
    println!(
        "hotlap REPL\n\n\
         Usage: hotlap [--state-dir DIR] [--bootstrap HOST] [--checkpoint-interval SECS]\n\
         \n\
         Flags:\n\
           --state-dir DIR              directory for durable checkpoint state\n\
           --bootstrap HOST             default Fluss bootstrap for DDL\n\
           --checkpoint-interval SECS   periodic checkpoint interval"
    );
}
