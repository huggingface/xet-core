use xet_error::Error;

#[derive(Error, Debug)]
#[error(transparent)]
pub struct Error(String);

fn main() {}
