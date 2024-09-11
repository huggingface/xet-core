#![deny(deprecated, clippy::all, clippy::pedantic)]

use xet_error::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[deprecated]
    #[error("...")]
    Deprecated,
}
