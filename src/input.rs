use csv::{ByteRecord, ByteRecordsIntoIter};
use csv::Reader;
use std::fs::File;
use std::iter::FromIterator;
use encoding::all::ISO_8859_1;
use encoding::{EncodingRef, DecoderTrap};
use std::clone::Clone;

use super::Row;

fn decode(data: ByteRecord, encoding: EncodingRef) -> Row {
    let mut row = Row::with_capacity(data.as_slice().len(), data.len());

    for item in data.iter() {
        row.push_field(&encoding.decode(item, DecoderTrap::Replace).unwrap());
    }

    row
}

pub struct ReaderSource {
    pub reader: Reader<File>,
    pub path: String,
    pub encoding: EncodingRef,
}

impl ReaderSource {
    fn headers(&mut self) -> Row {
        let data = self.reader.byte_headers().unwrap().clone();
        let mut headers = decode(data, self.encoding);

        headers.push_field("_source");

        headers
    }
}

pub struct ByteRecordsIntoIterSource {
    pub records: ByteRecordsIntoIter<File>,
    pub path: String,
    pub encoding: EncodingRef,
}

pub struct InputStream {
    readers: Vec<ReaderSource>,
    current_records: ByteRecordsIntoIterSource,
    headers: Row,
}

impl InputStream {
    fn new(mut reader_source: ReaderSource) -> InputStream {
        let headers = reader_source.headers();

        InputStream {
            readers: Vec::new(),
            headers,
            current_records: ByteRecordsIntoIterSource {
                path: reader_source.path,
                records: reader_source.reader.into_byte_records(),
                encoding: reader_source.encoding,
            },
        }
    }

    fn add(&mut self, item: ReaderSource) {
        self.readers.push(item);
    }

    pub fn headers(&self) -> &Row {
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
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_records.records.next() {
            Some(Ok(reg)) => {
                let mut str_reg = decode(reg, self.current_records.encoding);
                str_reg.push_field(&self.current_records.path);

                if str_reg.len() != self.headers.len() {
                    panic!("Inconsistent size of rows");
                }

                Some(str_reg)
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
                        encoding: rs.encoding,
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
    use super::{ReaderSource, InputStream, Row};
    use encoding::all::{UTF_8, WINDOWS_1252};

    #[test]
    fn test_read_concatenated() {
        let filenames = ["test/assets/1.csv", "test/assets/2.csv"];
        let mut input_stream: InputStream = filenames
            .iter()
            .filter_map(|f| {
                Some(ReaderSource {
                    reader: csv::Reader::from_path(f).unwrap(),
                    path: f.to_string(),
                    encoding: UTF_8,
                })
            })
            .collect();

        assert_eq!(*input_stream.headers(), Row::from(vec!["a", "b", "_source"]));

        assert_eq!(input_stream.next(), Some(Row::from(vec!["1", "3", "test/assets/1.csv"])));
        assert_eq!(input_stream.next(), Some(Row::from(vec!["5", "2", "test/assets/1.csv"])));
        assert_eq!(input_stream.next(), Some(Row::from(vec!["2", "2", "test/assets/2.csv"])));
        assert_eq!(input_stream.next(), Some(Row::from(vec!["4", "3", "test/assets/2.csv"])));
    }

    #[test]
    fn different_encoding() {
        let filenames = ["test/assets/windows1252/data.csv"];
        let mut input_stream: InputStream = filenames
            .iter()
            .filter_map(|f| {
                Some(ReaderSource {
                    reader: csv::Reader::from_path(f).unwrap(),
                    path: f.to_string(),
                    encoding: WINDOWS_1252,
                })
            })
            .collect();

        assert_eq!(*input_stream.headers(), Row::from(vec!["name", "_source"]));

        assert_eq!(input_stream.next(), Some(Row::from(vec!["árbol", "test/assets/windows1252/data.csv"])));
    }
}
