use super::{AggregateError, Aggregate};

#[derive(Default,Debug)]
pub struct Min {
    current: f64,
}

impl Min {
    pub fn new() -> Min {
        Default::default()
    }
}

impl Clone for Min {
    fn clone(&self) -> Min {
        Min::new()
    }
}

impl Aggregate for Min {
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
    use super::{Min, Aggregate};

    #[test]
    fn test_min() {
        let mut min = Min::new();

        min.update("3.0");
        min.update("2");
        min.update(".5");

        assert_eq!(min.value(), ".5");
    }
}
