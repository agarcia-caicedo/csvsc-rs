use super::{AggregateError, Aggregate};

#[derive(Default,Debug)]
pub struct Avg {
    sum: f64,
    count: u64,
}

impl Avg {
    pub fn new() -> Avg {
        Default::default()
    }
}

impl Clone for Avg {
    fn clone(&self) -> Avg {
        Avg::new()
    }
}

impl Aggregate for Avg {
    fn update(&mut self, data: &str) -> Result<(), AggregateError> {
        match data.parse::<f64>() {
            Ok(num) => {
                self.sum += num;
                self.count += 1;

                Ok(())
            },
            Err(_) => Err(AggregateError::Parse),
        }
    }

    fn value(&self) -> String {
        (self.sum / self.count as f64).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Avg, Aggregate};

    #[test]
    fn test_avg() {
        let mut Avg = Avg::new();

        Avg.update("3.0");
        Avg.update("2");
        Avg.update(".5");
        Avg.update(".5");

        assert_eq!(Avg.value(), "1.5");
    }
}
