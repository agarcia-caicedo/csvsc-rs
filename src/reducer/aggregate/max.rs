use super::{AggregateError, Aggregate};

#[derive(Default,Debug)]
pub struct Max {
    current: f64,
}

impl Max {
    pub fn new() -> Max {
        Default::default()
    }
}

impl Clone for Max {
    fn clone(&self) -> Max {
        Max::new()
    }
}

impl Aggregate for Max {
    fn update(&mut self, data: &str) -> Result<(), AggregateError> {
        match data.parse::<f64>() {
            Ok(num) => {
                if num < self.current {
                    self.current = num;
                }

                Ok(())
            },
            Err(_) => Err(AggregateError::Parse),
        }
    }

    fn value(&self) -> String {
        self.current.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Max, Aggregate};

    #[test]
    fn test_max() {
        let mut max = Max::new();

        max.update("3.0");
        max.update("2");
        max.update(".5");

        assert_eq!(max.value(), "3");
    }
}
