use std::iter::FromIterator;
use csv::Reader;
use std::fs::File;
use csv::ByteRecordsIntoIter;
use csv::ByteRecord;

pub struct ReaderSource {
    pub reader: Reader<File>,
    pub path: String,
}

pub struct InputStream {
    readers: Vec<ReaderSource>,
    current_reader: ByteRecordsIntoIter<File>,
    headers: ByteRecord,
}

impl InputStream {
    fn new(ReaderSource{mut reader, path}: ReaderSource) -> InputStream {
        InputStream{
            readers: Vec::new(),
            headers: reader.byte_headers().unwrap().clone(),
            current_reader: reader.into_byte_records(),
        }
    }

    fn add(&mut self, item: ReaderSource) {
        self.readers.push(item);
    }

    pub fn headers(&self) -> &ByteRecord {
        &self.headers
    }
}

impl FromIterator<ReaderSource> for InputStream {
    fn from_iter<I: IntoIterator<Item=ReaderSource>>(iter: I) -> Self {
        let mut iter = iter.into_iter();
        let mut ra:InputStream = InputStream::new(iter.next().expect("At least one input is required"));

        for item in iter {
            ra.add(item);
        }

        ra
    }
}

impl Iterator for InputStream {
    type Item = ByteRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_reader.next() {
            Some(Ok(reg)) => {
                if reg.len() != self.headers.len() {
                    panic!("Inconsistent size of rows");
                }

                Some(reg)
            },
            Some(Err(e)) => self.next(), // TODO warn something here
            None => {
                match self.readers.pop() {
                    Some(mut rs) => {
                        let new_headers = rs.reader.byte_headers().unwrap().clone();

                        if new_headers != self.headers {
                            panic!("Inconsistent headers among files");
                        }

                        self.current_reader = rs.reader.into_byte_records();

                        self.next()
                    },
                    None => None,
                }
            },
        }
    }
}
