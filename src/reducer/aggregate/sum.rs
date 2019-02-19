use super::{AggregateError, Aggregate};

#[derive(Default)]
pub struct Sum {
    total: f64,
}

impl Sum {
    pub fn new() -> Sum {
        Default::default()
    }
}

impl Clone for Sum {
    fn clone(&self) -> Sum {
        Sum { total: 0.0 }
    }
}

impl Aggregate for Sum {
    fn update(&mut self, data: &str) -> Result<(), AggregateError> {
        match data.parse::<f64>() {
            Ok(num) => Ok(self.total += num),
            Err(_) => Err(AggregateError::Parse),
        }
    }

    fn value(&self) -> String {
        self.total.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Sum, Aggregate};

    #[test]
    fn test_sum() {
        let mut sum = Sum::new();

        sum.update("3.0");
        sum.update("2");
        sum.update(".5");

        assert_eq!(sum.value(), "5.5");
    }
}
