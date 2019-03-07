use std::collections::HashMap;
use csv::Writer;
use super::{RowResult, Row, RowStream, Headers, get_field, TARGET_FIELD, Error};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

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
    targets: HashMap<PathBuf, Writer<File>>,
    headers: Headers,
    iter: I,
}

impl<I> IntoIter<I> {
    fn get_target(&mut self, row: &Row) -> Result<&mut Writer<File>, Error> {
        let header_row = self.headers.as_row();

        // TODO things that might fail in the closure should cause the Err variant
        // in this function's return value
        Ok(match get_field(&self.headers, row, TARGET_FIELD) {
            Some(target) => self.targets.entry(PathBuf::from(target)).or_insert_with(|| {
                let dirname = Path::new(target).parent().expect("Does not have a parent");
                fs::create_dir_all(dirname).expect("Could not create directory");

                let mut writer = Writer::from_path(target).expect(&format!("Cannot write to target {}", target));

                writer.write_record(header_row).expect("Could not write headers");

                writer
            }),
            None => self.targets.entry(PathBuf::from("/dev/stdout")).or_insert_with(|| {
                let mut writer = Writer::from_path("/dev/stdout").expect("Could not write to /dev/stdout");

                writer.write_record(header_row).expect("Could not write headers");

                writer
            }),
        })
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
