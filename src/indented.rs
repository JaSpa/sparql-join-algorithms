use std::fmt::{Display, Formatter, Result, Write};

pub struct Indented<'s, A> {
    value: A,
    indent: &'s str,
}

pub fn indented<A>(value: A) -> Indented<'static, A> {
    indented_by(value, "    ")
}

pub fn indented_by<A>(value: A, indent: &str) -> Indented<'_, A> {
    Indented { value, indent }
}

struct IndentedFmt<'a, F> {
    inner: &'a mut F,
    indent: &'a str,
}

impl<A> Display for Indented<'_, A>
where
    A: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut indented_fmt = IndentedFmt {
            inner: f,
            indent: self.indent,
        };
        write!(indented_fmt, "{}{}", self.indent, self.value)
    }
}

impl<F> Write for IndentedFmt<'_, F>
where
    F: Write,
{
    fn write_str(&mut self, s: &str) -> Result {
        for (i, line) in s.split('\n').enumerate() {
            if i > 0 {
                self.inner.write_char('\n')?;
                self.inner.write_str(self.indent)?;
            }
            self.inner.write_str(line)?
        }
        Ok(())
    }
}
