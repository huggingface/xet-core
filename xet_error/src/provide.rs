use std::error::{Error, Request};

#[doc(hidden)]
pub trait ThiserrorProvide: Sealed {
    fn xet_error_provide<'a>(&'a self, request: &mut Request<'a>);
}

impl<T> ThiserrorProvide for T
where
    T: Error + ?Sized,
{
    #[inline]
    fn xet_error_provide<'a>(&'a self, request: &mut Request<'a>) {
        self.provide(request);
    }
}

#[doc(hidden)]
pub trait Sealed {}
impl<T: Error + ?Sized> Sealed for T {}
