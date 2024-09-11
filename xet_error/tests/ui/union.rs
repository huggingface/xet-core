use xet_error::Error;

#[derive(Error)]
pub union U {
    msg: &'static str,
    num: usize,
}

fn main() {}
