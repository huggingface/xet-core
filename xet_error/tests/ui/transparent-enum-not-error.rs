use xet_error::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Other { message: String },
}

fn main() {}
