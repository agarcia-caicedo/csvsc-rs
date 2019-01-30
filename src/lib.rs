pub mod columns;
pub mod input;

pub use columns::AddColumns;
pub use input::InputStream;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
