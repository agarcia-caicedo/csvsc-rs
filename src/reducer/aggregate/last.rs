use super::{AggregateError, Aggregate};

#[derive(Default,Debug)]
pub struct Last {
    current: String,
}

impl Last {
    pub fn new() -> Last {
        Default::default()
    }
}

impl Clone for Last {
    fn clone(&self) -> Last {
        Last::new()
    }
}

impl Aggregate for Last {
    fn update(&mut self, data: &str) -> Result<(), AggregateError> {
        self.current.replace_range(.., data);

        Ok(())
    }

    fn value(&self) -> String {
        self.current.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{Last, Aggregate};

    #[test]
    fn test_last() {
        let mut sum = Last::new();

        sum.update("3.0");
        sum.update("2");
        sum.update(".5");

        assert_eq!(sum.value(), ".5");
    }
}
