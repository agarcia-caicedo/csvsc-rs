use std::iter::FromIterator;
use csv::ByteRecord;

pub struct InputStream<T> {
    array: Vec<T>,
}

impl<T> InputStream<T> {
    fn new() -> InputStream<T> {
        InputStream{ array: Vec::new() }
    }

    fn add(&mut self, item: T) {
        self.array.push(item);
    }
}

impl<T> FromIterator<T> for InputStream<T> {
    fn from_iter<I: IntoIterator<Item=T>>(iter: I) -> Self {
        let mut ra:InputStream<T> = InputStream::new();

        for item in iter {
            ra.add(item);
        }

        ra
    }
}

impl<T> Iterator for InputStream<T> {
    type Item = ByteRecord;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
