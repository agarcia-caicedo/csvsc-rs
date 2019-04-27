use std::rc::Rc;
use std::str::FromStr;
use super::aggregate::{self, Aggregate};

/// Kinds of errors that could happend when creating an AggregatedCol.
#[derive(Debug)]
pub enum AggregatedColParseError {
    /// The specified string is not composed of exactly three words separated
    /// by two colons
    WrongNumberOfParts,

    /// The given aggregate is not in the known list
    UnknownAggregate(String),
}

/// Instances of this struct define columns with aggregated values to be added
/// to each row of the stream.
///
/// The best method for creating an AggregatedCol is by parsing a string with
/// the spec of the new column,
#[derive(Clone)]
pub struct AggregatedCol {
    colname: String,
    source: Rc<String>,
    aggregate: Box<dyn Aggregate>,
}

impl AggregatedCol {
    /// Manually create an AggregatedCol by its ingredients which are the
    /// new column's name, the soure column's name and an implementor of the
    /// [Aggregate] trait.
    pub fn new(colname: &str, source: Rc<String>, aggregate: Box<dyn Aggregate>) -> AggregatedCol {
        AggregatedCol {
            colname: colname.to_string(),
            source,
            aggregate,
        }
    }

    /// Get this aggregate's colname
    pub fn colname(&self) -> &str {
        &self.colname
    }

    /// Get this aggregate's source colname
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Get this aggregate's Aggregate
    pub fn aggregate(&self) -> &Box<dyn Aggregate> {
        &self.aggregate
    }
}

impl FromStr for AggregatedCol {
    type Err = AggregatedColParseError;

    fn from_str(def: &str) -> Result<AggregatedCol, Self::Err> {
        let pieces: Vec<&str> = def.split(':').collect();

        if pieces.len() != 3 {
            return Err(AggregatedColParseError::WrongNumberOfParts);
        }

        let source = Rc::new(pieces[2].to_string());

        Ok(AggregatedCol {
            colname: pieces[0].to_string(),
            aggregate: match pieces[1] {
                "sum" => Box::new(aggregate::Sum::new(Rc::clone(&source))),
                "last" => Box::new(aggregate::Last::new(Rc::clone(&source))),
                "avg" => Box::new(aggregate::Avg::new(Rc::clone(&source))),
                "min" => Box::new(aggregate::Min::new(Rc::clone(&source))),
                "max" => Box::new(aggregate::Max::new(Rc::clone(&source))),
                s => return Err(AggregatedColParseError::UnknownAggregate(s.to_string())),
            },
            source,
        })
    }
}
