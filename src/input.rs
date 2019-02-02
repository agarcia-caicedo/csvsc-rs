use csv::Reader;
use csv::{ByteRecord, ByteRecordsIntoIter};
use encoding::all::ISO_8859_1;
use encoding::{DecoderTrap, EncodingRef};
use std::clone::Clone;
use std::fs::File;
use std::iter::FromIterator;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use crate::error::{Error, RowResult};

use super::Row;

fn decode(data: ByteRecord, encoding: EncodingRef) -> Row {
    let mut row = Row::with_capacity(data.as_slice().len(), data.len());

    for item in data.iter() {
        row.push_field(&encoding.decode(item, DecoderTrap::Replace).unwrap());
    }

    row
}

pub struct ReaderSource {
    reader: Reader<File>,
    path: PathBuf,
}

impl ReaderSource {
    pub fn from_reader<P: AsRef<Path>>(reader: Reader<File>, path: P) -> ReaderSource {
        ReaderSource {
            reader,
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<ReaderSource, csv::Error> {
        Ok(ReaderSource::from_reader(
            csv::Reader::from_path(&path)?,
            path,
        ))
    }

    fn headers(&mut self) -> ByteRecord {
        let mut data = self.reader.byte_headers().unwrap().clone();
        data.push_field(b"_source");

        data
    }
}

pub struct InputStream {
    readers: VecDeque<ReaderSource>,
    current_records: ByteRecordsIntoIter<File>,
    current_path: PathBuf,
    encoding: EncodingRef,
    headers: Row,
}

impl InputStream {
    pub fn from_readers<I>(readers: I, encoding: EncodingRef) -> InputStream
    where
        I: IntoIterator<Item = ReaderSource>,
    {
        let mut iter = readers.into_iter();
        let mut input_stream: InputStream = InputStream::new(
            iter.next().expect("At least one input is required"),
            encoding,
        );

        for item in iter {
            input_stream.add(item);
        }

        input_stream
    }

    fn new(mut reader_source: ReaderSource, encoding: EncodingRef) -> InputStream {
        InputStream {
            readers: VecDeque::new(),
            headers: decode(reader_source.headers(), encoding),
            current_records: reader_source.reader.into_byte_records(),
            current_path: reader_source.path,
            encoding,
        }
    }

    fn add(&mut self, item: ReaderSource) {
        self.readers.push_back(item);
    }

    pub fn headers(&self) -> &Row {
        &self.headers
    }
}

impl Iterator for InputStream {
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_records.next() {
            Some(Ok(reg)) => {
                let mut str_reg = decode(reg, self.encoding);
                str_reg.push_field(&self.current_path.to_string_lossy());

                if str_reg.len() != self.headers.len() {
                    return Some(Err(Error::InconsistentSizeOfRows(
                        self.current_path.clone(),
                    )));
                }

                Some(Ok(str_reg))
            }

            Some(Err(e)) => Some(Err(Error::Csv(e))),

            None => match self.readers.pop_front() {
                Some(mut rs) => {
                    let new_headers = decode(rs.headers(), self.encoding);

                    if new_headers != self.headers {
                        return Some(Err(Error::InconsistentHeaders(PathBuf::from(rs.path))));
                    }

                    self.current_records = rs.reader.into_byte_records();
                    self.current_path = rs.path;

                    self.next()
                }

                None => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InputStream, ReaderSource, Row};
    use crate::error::Error;
    use encoding::all::{UTF_8, WINDOWS_1252};

    use std::path::PathBuf;
    use std::str::FromStr;

    #[test]
    fn test_read_concatenated() {
        let filenames = ["test/assets/1.csv", "test/assets/2.csv"];
        let mut input_stream = InputStream::from_readers(
            filenames
                .iter()
                .filter_map(|f| Some(ReaderSource::from_path(f).unwrap())),
            UTF_8,
        );

        assert_eq!(
            *input_stream.headers(),
            Row::from(vec!["a", "b", "_source"])
        );

        assert_eq!(
            input_stream.next().unwrap().unwrap(),
            Row::from(vec!["1", "3", "test/assets/1.csv"])
        );
        assert_eq!(
            input_stream.next().unwrap().unwrap(),
            Row::from(vec!["5", "2", "test/assets/1.csv"])
        );
        assert_eq!(
            input_stream.next().unwrap().unwrap(),
            Row::from(vec!["2", "2", "test/assets/2.csv"])
        );
        assert_eq!(
            input_stream.next().unwrap().unwrap(),
            Row::from(vec!["4", "3", "test/assets/2.csv"])
        );
    }

    #[test]
    fn different_encoding() {
        let filenames = ["test/assets/windows1252/data.csv"];
        let mut input_stream = InputStream::from_readers(
            filenames
                .iter()
                .filter_map(|f| Some(ReaderSource::from_path(f).unwrap())),
            WINDOWS_1252,
        );

        assert_eq!(*input_stream.headers(), Row::from(vec!["name", "_source"]));

        assert_eq!(
            input_stream.next().unwrap().unwrap(),
            Row::from(vec!["árbol", "test/assets/windows1252/data.csv"])
        );
    }

    #[test]
    fn detects_inconsistent_headers() {
        let filenames = ["test/assets/1.csv", "test/assets/3.csv"];
        let input_stream = InputStream::from_readers(
            filenames
                .iter()
                .filter_map(|f| Some(ReaderSource::from_path(f).unwrap())),
            UTF_8,
        );

        match input_stream.skip(2).next() {
            Some(Err(Error::InconsistentHeaders(ref p)))
                if *p == PathBuf::from_str("test/assets/3.csv").unwrap() =>
            {
                ()
            }

            x => unreachable!("{:?}", x),
        }
    }
}
