use crate::error::RowResult;

use crate::{Headers, RowStream};

/// Allows calling a closure on each row, just like in rust's Iterator trait.
pub struct Inspect<I, F> {
    iter: I,
    f: F,
    headers: Headers,
}

impl<I, F> Inspect<I, F>
where
    I: RowStream,
    F: FnMut(&RowResult),
{
    pub fn new(iter: I, f: F) -> Inspect<I, F> {
        let headers = iter.headers().clone();

        Inspect { iter, f, headers }
    }
}

pub struct IntoIter<I, F> {
    iter: I,
    f: F,
}

impl<I, F> Iterator for IntoIter<I, F>
where
    I: Iterator<Item = RowResult>,
    F: FnMut(&RowResult),
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();

        if let Some(ref a) = next {
            (self.f)(a);
        }

        next
    }
}

impl<I, F> IntoIterator for Inspect<I, F>
where
    I: RowStream,
    F: FnMut(&RowResult),
{
    type Item = RowResult;

    type IntoIter = IntoIter<I::IntoIter, F>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            iter: self.iter.into_iter(),
            f: self.f,
        }
    }
}

impl<I, F> RowStream for Inspect<I, F>
where
    I: RowStream,
    F: FnMut(&RowResult),
{
    fn headers(&self) -> &Headers {
        &self.headers
    }
}
