use super::{Aggregate, aggregated_col::AggregatedCol};
use crate::{Headers, Row};

pub struct Group {
    contents: Vec<Box<dyn Aggregate>>,
}

impl Group {
    pub fn update(&mut self, headers: &Headers, row: &Row) {
        for agg in self.contents.iter_mut() {
            agg.update(headers.get_field(row, agg.source()).unwrap())
                .expect("could not update");
        }
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

impl<'a> From<&'a Vec<AggregatedCol>> for Group {
    fn from(row: &'a Vec<AggregatedCol>) -> Group {
        let mut contents = Vec::with_capacity(row.len());

        for item in row {
            contents.push((*item.aggregate()).clone());
        }

        Group { contents }
    }
}
