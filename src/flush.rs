use csv::Writer;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::result;
use crate::{
    get_field, Error, Headers, Row, RowResult, RowStream,
    error::Result,
};

fn trim_underscores(headers: &Headers, row: &Row) -> Row {
    // FIXME bad estimate here of row size
    let mut new_row = Row::with_capacity(row.as_slice().len(), row.len());

    for (h, f) in headers.iter().zip(row.iter()) {
        if !h.starts_with('_') {
            new_row.push_field(f);
        }
    }

    new_row
}

fn trim_header_underscores(headers: &Row) -> Row {
    let mut new_row = Row::with_capacity(
        headers.iter().filter(|h| !h.starts_with('_')).map(|h| h.len()).fold(0, |acc, n| acc + n),
        headers.iter().filter(|h| !h.starts_with('_')).count()
    );

    for h in headers.iter().filter(|h| !h.starts_with('_')) {
        new_row.push_field(h);
    }

    new_row
}

pub enum FlushTarget {
    Column(String),
    Path(PathBuf),
}

/// Flushes the rows to the destination specified by a column.
///
/// Fields starting with underscore are not written.
pub struct Flush<I> {
    iter: I,
    target: FlushTarget,
}

impl<I> Flush<I>
where
    I: RowStream,
{
    pub fn new(iter: I, target: FlushTarget) -> Result<Flush<I>> {
        if let FlushTarget::Column(s) = target {
            if !iter.headers().contains_key(&s) {
                return Err(Error::ColumnNotFound(s));
            }

            Ok(Flush { iter, target: FlushTarget::Column(s) })
        } else {
            Ok(Flush { iter, target })
        }
    }
}

pub struct IntoIter<I> {
    targets: HashMap<PathBuf, Writer<File>>,
    headers: Headers,
    target: FlushTarget,
    iter: I,
}

impl<I> IntoIter<I> {
    fn get_target(&mut self, row: &Row) -> result::Result<&mut Writer<File>, Error> {
        let header_row = trim_header_underscores(self.headers.as_row());

        // TODO think about returning an error instead of unwrapping maybe
        match self.target {
            FlushTarget::Column(ref colname) => {
                // can unwrap because we checked the existence of the field
                // while building the Flush
                let target = PathBuf::from(get_field(&self.headers, row, &colname).unwrap());

                if self.targets.contains_key(&target) {
                    Ok(self.targets.get_mut(&target).unwrap())
                } else {
                    let dirname = Path::new(&target).parent().expect("Does not have a parent");
                    fs::create_dir_all(dirname).expect("Could not create directory");

                    let mut writer = Writer::from_path(&target)
                        .expect(&format!("Cannot write to target {:?}", &target));

                    writer
                        .write_record(&header_row)
                        .expect("Could not write headers");

                    self.targets.insert(target.to_path_buf(), writer);

                    Ok(self.targets.get_mut(&target).unwrap())
                }
            },
            FlushTarget::Path(ref path) => {
                if self.targets.contains_key(path) {
                    Ok(self.targets.get_mut(path).unwrap())
                } else {
                    let mut writer =
                        Writer::from_path(path).expect("Could not write to file");

                    writer
                        .write_record(&header_row)
                        .expect("Could not write headers");

                    self.targets.insert(path.to_path_buf(), writer);

                    Ok(self.targets.get_mut(path).unwrap())
                }
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
                let trimmed_row = trim_underscores(&self.headers, &row);

                match self.get_target(&row) {
                    Ok(target) => match target.write_record(&trimmed_row) {
                        Ok(_) => Some(Ok(row)),
                        Err(e) => Some(Err(Error::Csv(e))),
                    },
                    Err(err) => Some(Err(err)),
                }
            },
            err @ Some(Err(_)) => err,
            None => None,
        }
    }
}

impl<I> IntoIterator for Flush<I>
where
    I: RowStream,
{
    type Item = RowResult;

    type IntoIter = IntoIter<I::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            targets: HashMap::new(),
            target: self.target,
            headers: self.iter.headers().clone(),
            iter: self.iter.into_iter(),
        }
    }
}
