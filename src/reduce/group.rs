use super::{Aggregate, AggregateError};
use crate::{Headers, Row};

pub struct Group {
    contents: Vec<Box<dyn Aggregate>>,
}

impl Group {
    pub fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), AggregateError> {
        for agg in self.contents.iter_mut() {
            agg.update(headers, row)?
        }

        Ok(())
    }

    pub fn as_row(self) -> Row {
        let contents: Vec<String> = self.contents.iter().map(|g| g.value()).collect();
        let buff_size = contents.iter().map(|s| s.len()).fold(0, |acc, n| acc + n);
        let mut row = Row::with_capacity(buff_size, contents.len());

        for item in contents.into_iter() {
            row.push_field(&item);
        }

        row
    }
}

impl<'a> From<Vec<Box<dyn Aggregate>>> for Group {
    fn from(contents: Vec<Box<dyn Aggregate>>) -> Group {
        Group { contents }
    }
}
