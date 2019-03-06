use std::collections::HashMap;
use super::{Row, RowResult, Headers, ColSpec, AddColumns};

pub fn get_field<'r>(headers: &Headers, row: &'r Row, field: &str) -> Option<&'r str> {
    headers.get(field).and_then(|i| row.get(i))
}

pub trait RowStream: IntoIterator<Item = RowResult> {
    fn headers(&self) -> &Headers;
}

pub fn pack<'a>(headers: &'a Headers, row: &'a Row) -> HashMap<&'a str, &'a str> {
    headers.iter().zip(row.iter()).collect()
}

#[cfg(test)]
mod tests {
    use super::{get_field, Headers, Row, pack};

    #[test]
    fn test_get_field() {
        let headers = Headers::from_row(Row::from(vec!["id", "val"]));
        let row = Row::from(vec!["1", "40"]);

        assert_eq!(get_field(&headers, &row, "id"), Some("1"));
        assert_eq!(get_field(&headers, &row, "val"), Some("40"));
        assert_eq!(get_field(&headers, &row, "foo"), None);
    }

    #[test]
    fn test_pack() {
        let headers = Headers::from_row(Row::from(vec!["id", "val"]));
        let row = Row::from(vec!["1", "40"]);

        let result = pack(&headers, &row);
    }
}
