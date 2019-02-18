use std::collections::{hash_map, HashMap};
use crate::{Row, RowResult};
use super::{Aggregate, AggregatedCol};

pub struct Group {
    contents: Vec<Box<dyn Aggregate>>,
}

impl Group {
    pub fn update(&mut self, row: &Row) {
        for (agg, data) in self.contents.iter_mut().zip(row.iter()) {
            agg.update(data);
        }
    }

    pub fn as_row(self) -> Row {
        unimplemented!()
    }
}

impl<'a> From<&'a Vec<AggregatedCol>> for Group {
    fn from(row: &'a Vec<AggregatedCol>) -> Group {
        let mut contents = Vec::with_capacity(row.len());

        for item in row {
            contents.push((*item.aggregate()).clone());
        }

        Group {
            contents,
        }
    }
}

pub struct Groups {
    groups: hash_map::IntoIter<u64, Group>,
}

impl From<HashMap<u64, Group>> for Groups {
    fn from(data: HashMap<u64, Group>) -> Groups {
        Groups {
            groups: data.into_iter(),
        }
    }
}

impl Iterator for Groups {
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.groups.next().map(|g| {
            Ok(g.1.as_row())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Group;

    #[test]
    fn test_group_as_row() {
        unimplemented!()
    }
}
