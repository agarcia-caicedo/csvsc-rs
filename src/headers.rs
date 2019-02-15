use std::collections::HashMap;
use std::cmp::PartialEq;
use csv::StringRecordIter;

use super::Row;

#[derive(Debug,Clone,PartialEq)]
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

    pub fn add(&mut self, colname: &str) {
        self.names.push_field(colname);
        self.indexes.insert(colname.to_string(), self.names.len()-1);
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn as_row(&self) -> &Row {
        &self.names
    }

    pub fn get(&self, field: &str) -> Option<usize> {
        self.indexes.get(field).map(|u| *u)
    }

    pub fn iter(&self) -> StringRecordIter {
        self.names.iter()
    }
}

impl PartialEq<Headers> for Row {
    fn eq(&self, other: &Headers) -> bool {
        self == other.as_row()
    }
}
