use std::{
    any::Any,
    fmt::{self, Debug},
    panic,
};

pub trait ExpectFmt {
    type Unwrapped;
    fn expect_fmt(self, msg: fmt::Arguments<'_>) -> Self::Unwrapped;
}

impl<Value, Error> ExpectFmt for Result<Value, Error>
where
    Error: Debug,
{
    type Unwrapped = Value;

    #[inline]
    fn expect_fmt(self, msg: fmt::Arguments<'_>) -> Self::Unwrapped {
        match self {
            Ok(value) => value,
            Err(error) => unwrap_failed(&msg, &error),
        }
    }
}

#[cold]
#[inline(never)]
fn unwrap_failed(msg: &fmt::Arguments<'_>, error: &dyn fmt::Debug) -> ! {
    panic!("{msg}: {error:?}")
}

pub trait ExpectUnwind {
    type Unwrapped;
    fn expect_unwind(self) -> Self::Unwrapped;
}

impl<Value> ExpectUnwind for Result<Value, Box<dyn Any + Send>> {
    type Unwrapped = Value;

    fn expect_unwind(self) -> Self::Unwrapped {
        match self {
            Ok(value) => value,
            Err(payload) => panic::resume_unwind(payload),
        }
    }
}

impl ExpectUnwind for Option<Box<dyn Any + Send>> {
    type Unwrapped = ();

    fn expect_unwind(self) -> Self::Unwrapped {
        if let Some(payload) = self {
            panic::resume_unwind(payload)
        }
    }
}
