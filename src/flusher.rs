use std::collections::HashMap;
use csv::Writer;
use super::{RowResult, Row, RowStream, Headers, get_field, TARGET_FIELD, Error};
use std::fs::File;

pub struct Flusher<I> {
    iter: I,
}

impl<I> Flusher<I>
where
    I: RowStream,
{
    pub fn new(iter: I) -> Flusher<I> {
        Flusher {
            iter,
        }
    }
}

pub struct IntoIter<I> {
    targets: HashMap<String, Writer<File>>,
    headers: Headers,
    iter: I,
}

impl<I> IntoIter<I> {
    fn get_target(&mut self, row: &Row) -> Result<&mut Writer<File>, Error> {
        let header_row = self.headers.as_row();

        match get_field(&self.headers, row, TARGET_FIELD) {
            Some(target) => {
                Ok(self.targets.entry(target.to_string()).or_insert_with(|| {
                    let mut writer = Writer::from_path(target).expect(&format!("Cannot write to target {}", target));

                    writer.write_record(header_row).expect("Could not write headers");

                    writer
                }))
            },
            None => {
                Ok(self.targets.entry("stdout".to_string()).or_insert_with(|| {
                    let mut writer = Writer::from_path("/dev/stdout").expect("Could not write to /dev/stdout");

                    writer.write_record(header_row).expect("Could not write headers");

                    writer
                }))
            },
        }
    }
}

impl<I> Iterator for IntoIter<I>
where
    I: Iterator<Item = RowResult>,
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(row)) => {
                match self.get_target(&row) {
                    Ok(target) => {
                        match target.write_record(&row) {
                            Ok(_) => Some(Ok(row)),
                            Err(e) => Some(Err(Error::Csv(e))),
                        }
                    },
                    Err(err) => Some(Err(err)),
                }
            },
            err@Some(Err(_)) => err,
            None => None,
        }
    }
}

impl<I> IntoIterator for Flusher<I>
where
    I: RowStream,
{
    type Item = RowResult;

    type IntoIter = IntoIter<I::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            targets: HashMap::new(),
            headers: self.iter.headers().clone(),
            iter: self.iter.into_iter(),
        }
    }
}
