use std::iter::FromIterator;
use csv::Reader;
use std::fs::File;
use csv::ByteRecordsIntoIter;
use csv::ByteRecord;

pub struct InputStream {
    readers: Vec<Reader<File>>,
    current_reader: Option<ByteRecordsIntoIter<File>>,
    headers: Option<ByteRecord>,
}

impl InputStream {
    fn new() -> InputStream {
        InputStream{
            readers: Vec::new(),
            headers: None,
            current_reader: None,
        }
    }

    fn add(&mut self, item: Reader<File>) {
        self.readers.push(item);
    }
}

impl FromIterator<Reader<File>> for InputStream {
    fn from_iter<I: IntoIterator<Item=Reader<File>>>(iter: I) -> Self {
        let mut ra:InputStream = InputStream::new();

        for item in iter {
            ra.add(item);
        }

        ra
    }
}

impl Iterator for InputStream {
    type Item = ByteRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.current_reader {
            Some(records) => {
                match records.next() {
                    Some(Ok(reg)) => {
                        if reg.len() != self.headers.as_ref().unwrap().len() {
                            panic!("Inconsistent size of rows");
                        }

                        Some(reg)
                    },
                    Some(Err(e)) => self.next(), // TODO warn something here
                    None => {
                        self.current_reader = None;

                        self.next()
                    },
                }
            },
            None => match self.readers.pop() {
                Some(mut reader) => {
                    let new_headers = reader.byte_headers().unwrap().clone();

                    if let Some(ref headers) = self.headers {
                        if new_headers != *headers {
                            panic!("Inconsistent headers among files");
                        }
                    } else {
                        self.headers = Some(new_headers);
                    }

                    self.current_reader = Some(reader.into_byte_records());

                    self.next()
                },
                None => None,
            }
        }
    }
}
