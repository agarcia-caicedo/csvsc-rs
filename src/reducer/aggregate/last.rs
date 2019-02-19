use std::rc::Rc;
use super::{AggregateError, Aggregate};

#[derive(Default,Debug)]
pub struct Last {
    source: Rc<String>,
    current: String,
}

impl Last {
    pub fn new(source: Rc<String>) -> Last {
        Last {
            source,
            ..Default::default()
        }
    }
}

impl Clone for Last {
    fn clone(&self) -> Last {
        Last::new(Rc::clone(&self.source))
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

    fn source(&self) -> &str {
        &self.source
    }
}

#[cfg(test)]
mod tests {
    use super::{Last, Aggregate};
    use std::rc::Rc;

    #[test]
    fn test_last() {
        let mut sum = Last::new(Rc::new("".to_string()));

        sum.update("3.0");
        sum.update("2");
        sum.update(".5");

        assert_eq!(sum.value(), ".5");
    }
}
