use std::collections::HashMap;
use std::cmp::PartialEq;

use super::Row;

pub struct Headers {
    indexes: HashMap<String, usize>,
    names: Row,
}

impl Headers {
    pub fn from_row(row: Row) -> Headers {
        let mut indexes = HashMap::new();

        for (index, entry) in row.iter().enumerate() {
            indexes.insert(entry.to_string(), index);
        }

        Headers {
            indexes,
            names: row,
        }
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn as_row(&self) -> &Row {
        &self.names
    }
}

impl PartialEq<Headers> for Row {
    fn eq(&self, other: &Headers) -> bool {
        self == other.as_row()
    }
}