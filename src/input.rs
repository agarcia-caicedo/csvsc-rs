use std::iter::FromIterator;
use csv::ByteRecord;
use csv::Reader;

pub struct InputStream<T> {
    array: Vec<Reader<T>>,
    current: usize,
}

impl<T> InputStream<T> {
    fn new() -> InputStream<T> {
        InputStream{ array: Vec::new(), current: 0 }
    }

    fn add(&mut self, item: Reader<T>) {
        self.array.push(item);
    }
}

impl<T> FromIterator<Reader<T>> for InputStream<T> {
    fn from_iter<I: IntoIterator<Item=Reader<T>>>(iter: I) -> Self {
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
