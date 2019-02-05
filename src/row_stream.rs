use super::Headers;

pub trait RowStream {
    fn headers(&self) -> &Headers;
}
