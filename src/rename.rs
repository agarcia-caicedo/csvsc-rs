use std::collections::HashMap;

use super::{Headers, RowStream, Row};

pub struct Rename<I> {
    iter: I,
    headers: Headers,
}

impl <I> Rename<I>
where
    I: RowStream,
{
    pub fn new(iter: I, name_map: &HashMap<&str, &str>) -> Rename<I> {
        let header_row = iter.headers().as_row();
        let mut new_headers = Row::new();

        for header in header_row {
            if name_map.contains_key(header) {
                new_headers.push_field(name_map.get(header).unwrap());
            } else {
                new_headers.push_field(header);
            }
        }

        Rename{
            iter,
            headers: Headers::from_row(new_headers),
        }
    }
}
