use csv::ByteRecord;
use csv::ByteRecordsIntoIter;
use csv::Reader;
use std::fs::File;
use std::iter::FromIterator;

pub struct ReaderSource {
    pub reader: Reader<File>,
    pub path: String,
}

impl ReaderSource {
    fn headers(&mut self) -> ByteRecord {
        let mut headers = self.reader.byte_headers().unwrap().clone();

        headers.push_field(b"_source");

        headers
    }
}

pub struct ByteRecordsIntoIterSource {
    pub records: ByteRecordsIntoIter<File>,
    pub path: String,
}

pub struct InputStream {
    readers: Vec<ReaderSource>,
    current_records: ByteRecordsIntoIterSource,
    headers: ByteRecord,
}

impl InputStream {
    fn new(mut reader_source: ReaderSource) -> InputStream {
        let headers = reader_source.headers();

        InputStream {
            readers: Vec::new(),
            headers,
            current_records: ByteRecordsIntoIterSource{
                path: reader_source.path,
                records: reader_source.reader.into_byte_records(),
            },
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
    fn from_iter<I: IntoIterator<Item = ReaderSource>>(iter: I) -> Self {
        let mut iter = iter.into_iter();
        let mut ra: InputStream =
            InputStream::new(iter.next().expect("At least one input is required"));

        for item in iter {
            ra.add(item);
        }

        ra
    }
}

impl Iterator for InputStream {
    type Item = ByteRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_records.records.next() {
            Some(Ok(mut reg)) => {
                reg.push_field(self.current_records.path.as_bytes());

                if reg.len() != self.headers.len() {
                    panic!("Inconsistent size of rows");
                }

                Some(reg)
            }
            Some(Err(e)) => self.next(), // TODO warn something here
            None => match self.readers.pop() {
                Some(mut rs) => {
                    let new_headers = rs.headers();

                    if new_headers != self.headers {
                        panic!("Inconsistent headers among files");
                    }

                    self.current_records = ByteRecordsIntoIterSource{
                        path: rs.path,
                        records: rs.reader.into_byte_records(),
                    };

                    self.next()
                }
                None => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ReaderSource, InputStream};
    use csv::ByteRecord;

    #[test]
    fn test_read_concatenated() {
        let filenames = ["test/assets/1.csv", "test/assets/2.csv"];
        let mut input_stream: InputStream = filenames
            .iter()
            .filter_map(|f| {
                Some(ReaderSource {
                    reader: csv::Reader::from_path(f).unwrap(),
                    path: f.to_string(),
                })
            })
            .collect();

        assert_eq!(*input_stream.headers(), ByteRecord::from(vec!["a", "b", "_source"]));

        assert_eq!(input_stream.next(), Some(ByteRecord::from(vec!["1", "3", "test/assets/1.csv"])));
        assert_eq!(input_stream.next(), Some(ByteRecord::from(vec!["5", "2", "test/assets/1.csv"])));
        assert_eq!(input_stream.next(), Some(ByteRecord::from(vec!["2", "2", "test/assets/2.csv"])));
        assert_eq!(input_stream.next(), Some(ByteRecord::from(vec!["4", "3", "test/assets/2.csv"])));
    }
}
