use xet_error::Error;

#[derive(Error, Debug)]
#[error(transparent)]
pub struct Error {
    message: String,
}

fn main() {}
