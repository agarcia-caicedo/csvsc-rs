use csv::StringRecordIter;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::Row;

/// A structure for keeping relationship between the headers and their positions
#[derive(Debug, Clone, PartialEq)]
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

    /// Retrieves a field from a row given it's header name
    ///
    /// ```rust
    /// use csvsc::{Headers, Row};
    ///
    /// let headers = Headers::from_row(Row::from(vec!["id", "val"]));
    /// let row = Row::from(vec!["1", "40"]);
    ///
    /// assert_eq!(headers.get_field(&row, "id"), Some("1"));
    /// assert_eq!(headers.get_field(&row, "val"), Some("40"));
    /// assert_eq!(headers.get_field(&row, "foo"), None);
    /// ```
    pub fn get_field<'r>(&self, row: &'r Row, field: &str) -> Option<&'r str> {
        self.index(field).and_then(|i| row.get(i))
    }

    /// Creates a hash for the row given as first argument only considering the
    /// columns specified by the second argument.
    ///
    /// ```rust
    /// use csvsc::{Headers, Row};
    ///
    /// let headers = Headers::from_row(Row::from(vec!["id", "name", "val"]));
    /// let row = Row::from(vec!["1", "juan", "40"]);
    ///
    /// dbg!(headers.hash(&row, &["id".to_string(), "name".to_string()]));
    /// ```
    ///
    /// If no columns are specified, a random hash is chosen. If a column is not
    /// found its name is returned as String wrapped in the Err variant of the
    /// return value
    pub fn hash(&self, row: &Row, columns: &[String]) -> Result<u64, String> {
        if columns.len() == 0 {
            return Ok(rand::random());
        }

        let mut hasher = DefaultHasher::new();

        for col in columns {
            match self.get_field(row, col) {
                Some(field) => field.hash(&mut hasher),
                None => return Err(col.to_string()),
            }
        }

        Ok(hasher.finish())
    }

    /// Adds a new header. It'll fail if the header is already present
    ///
    /// ```rust
    /// use csvsc::{Headers, Row};
    ///
    /// let mut h = Headers::from_row(Row::from(vec!["name"]));
    ///
    /// h.add("value").unwrap();
    ///
    /// assert_eq!(h, Headers::from_row(Row::from(vec!["name", "value"])));
    ///
    /// assert_eq!(h.add("name"), Err(()));
    /// assert_eq!(h, Headers::from_row(Row::from(vec!["name", "value"])));
    /// ```
    pub fn add(&mut self, colname: &str) -> Result<(), ()> {
        if self.indexes.contains_key(colname) {
            return Err(());
        }

        self.names.push_field(colname);
        self.indexes
            .insert(colname.to_string(), self.names.len() - 1);

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn as_row(&self) -> &Row {
        &self.names
    }

    fn index(&self, field: &str) -> Option<usize> {
        self.indexes.get(field).map(|u| *u)
    }

    pub fn contains_key(&self, field: &str) -> bool {
        self.indexes.contains_key(field)
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

#[cfg(test)]
mod tests {
    use super::Headers;
    use crate::Row;

    #[test]
    fn test_hash() {
        let header = Headers::from_row(Row::from(vec!["a", "b"]));
        let data = Row::from(vec!["1", "2"]);
        let cols = vec!["a".to_string()];

        assert_eq!(
            header.hash(&data, &cols).unwrap(),
            16569625464242099095
        );

        let header = Headers::from_row(Row::from(vec!["a", "b"]));
        let data = Row::from(vec!["1", "2"]);
        let cols = vec!["a".to_string(), "b".to_string()];

        assert_eq!(
            header.hash(&data, &cols).unwrap(),
            15633344752900483833
        );

        let header = Headers::from_row(Row::from(vec!["a", "b"]));
        let data = Row::from(vec!["1", "2"]);
        let cols = vec!["d".to_string()];

        assert_eq!(
            header.hash(&data, &cols),
            Err("d".to_string())
        );
    }
}
