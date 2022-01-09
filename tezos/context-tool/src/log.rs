use std::io::Write;
use termcolor::{BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

pub fn print(prefix: &str, s: Option<String>) {
    let bufwtr = BufferWriter::stdout(ColorChoice::Always);
    let mut buffer = bufwtr.buffer();
    buffer
        .set_color(ColorSpec::new().set_fg(Some(Color::Green)))
        .unwrap();
    buffer.write_all(prefix.as_bytes()).unwrap();
    buffer.set_color(ColorSpec::new().set_fg(None)).unwrap();

    if let Some(s) = s.as_ref() {
        buffer.write_all(s.as_bytes()).unwrap();
    };

    buffer.write_all("\n".as_bytes()).unwrap();

    // writeln!(&mut buffer, prefix).unwrap();
    bufwtr.print(&buffer).unwrap();
}

/// Print logs on stdout with the prefix `[tezedge.tool]`
macro_rules! log {
    () => (print("[tezedge.tool]", None));
    ($($arg:tt)*) => ({
        print("[tezedge.tool] ", Some(format!("{}", format_args!($($arg)*))))
    })
}
