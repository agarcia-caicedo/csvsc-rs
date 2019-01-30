pub mod input;
pub mod columns;

pub use input::InputStream;
pub use columns::AddColumns;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
