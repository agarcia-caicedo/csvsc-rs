use super::{Row, RowResult, Headers, ColSpec, AddColumns};

pub fn get_field<'r>(headers: &Headers, row: &'r Row, field: &str) -> Option<&'r str> {
    headers.get(field).and_then(|i| row.get(i))
}

pub trait RowStream: IntoIterator<Item = RowResult> {
    fn headers(&self) -> &Headers;

    fn add_columns(self, columns: Vec<ColSpec>) -> AddColumns<Self>
    where
        Self: Sized,
    {
        AddColumns::new(self, columns)
    }
}

#[cfg(test)]
mod tests {
    use super::{get_field, Headers, Row};

    #[test]
    fn test_get_field() {
        let headers = Headers::from_row(Row::from(vec!["id", "val"]));
        let row = Row::from(vec!["1", "40"]);

        assert_eq!(get_field(&headers, &row, "id"), Some("1"));
        assert_eq!(get_field(&headers, &row, "val"), Some("40"));
        assert_eq!(get_field(&headers, &row, "foo"), None);
    }
}
