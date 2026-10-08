//! Line-oriented REPL: split statements, dispatch commands and render output.

use std::io::{BufRead, Write};

use hotlap_runtime::{QueryResult, Session};

use crate::render;

/// Prompt written before each line when the reader is interactive.
pub const PROMPT: &str = "hotlap> ";

/// Help text printed by `\?`.
const HELP: &str = "\
Commands:
  <sql>;            run a statement ending in ';' (or the end of the line)
  SHOW METRICS;     print engine metrics as a table
  CHECKPOINT;       take a checkpoint now
  \\q                quit
  \\?                show this help
";

/// Run the REPL over `session` until EOF or `\q`.
///
/// Statements are separated by `;` or by the end of an input line. When
/// `prompt` is set, a prompt is written before each line.
pub fn run<R, W>(
    session: &mut Session,
    mut reader: R,
    writer: &mut W,
    prompt: bool,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    let mut line = String::new();
    loop {
        line.clear();
        if prompt {
            writer.write_all(PROMPT.as_bytes())?;
            writer.flush()?;
        }
        if reader.read_line(&mut line)? == 0 {
            return Ok(());
        }
        if !run_line(session, writer, &line)? {
            return Ok(());
        }
    }
}

/// Execute every statement on one input line; `Ok(false)` means quit.
fn run_line<W: Write>(session: &mut Session, out: &mut W, line: &str) -> Result<bool, CliError> {
    for statement in line.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        if !dispatch(session, out, statement)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Dispatch one statement; `Ok(false)` means quit.
fn dispatch<W: Write>(
    session: &mut Session,
    out: &mut W,
    statement: &str,
) -> Result<bool, CliError> {
    match command(statement) {
        Command::Quit => Ok(false),
        Command::Help => {
            out.write_all(HELP.as_bytes())?;
            Ok(true)
        }
        Command::Checkpoint => {
            let id = session.checkpoint()?;
            writeln!(out, "checkpoint {id}")?;
            Ok(true)
        }
        Command::ShowMetrics => {
            render::metrics(out, &session.metrics())?;
            Ok(true)
        }
        Command::Sql(sql) => {
            execute(session, out, sql)?;
            Ok(true)
        }
    }
}

/// Recognize a meta-command, defaulting to plain SQL.
fn command(statement: &str) -> Command<'_> {
    if statement.eq_ignore_ascii_case("\\q") {
        Command::Quit
    } else if statement == "\\?" {
        Command::Help
    } else if statement.eq_ignore_ascii_case("CHECKPOINT") {
        Command::Checkpoint
    } else if statement.eq_ignore_ascii_case("SHOW METRICS") {
        Command::ShowMetrics
    } else {
        Command::Sql(statement)
    }
}

/// Run one SQL statement and render its outcome; errors are printed, not fatal.
fn execute<W: Write>(session: &mut Session, out: &mut W, sql: &str) -> Result<(), CliError> {
    match session.sql(sql) {
        Ok(QueryResult::Rows(batches)) => render::rows(out, &batches)?,
        Ok(QueryResult::Ack(message)) => {
            if !message.is_empty() {
                writeln!(out, "{message}")?;
            }
        }
        Err(error) => writeln!(out, "error: {error}")?,
    }
    Ok(())
}

/// A statement the REPL understands.
enum Command<'a> {
    Quit,
    Help,
    Checkpoint,
    ShowMetrics,
    Sql(&'a str),
}

/// Errors raised while running the REPL.
#[derive(Debug)]
pub enum CliError {
    /// Reading input or writing output failed.
    Io(std::io::Error),
    /// A command-level session operation failed (query errors are printed).
    Session(hotlap_runtime::SessionError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "i/o: {error}"),
            Self::Session(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Session(error) => Some(error),
        }
    }
}

impl From<std::io::Error> for CliError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<hotlap_runtime::SessionError> for CliError {
    fn from(error: hotlap_runtime::SessionError) -> Self {
        Self::Session(error)
    }
}
