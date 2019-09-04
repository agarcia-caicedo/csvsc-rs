use std::str::FromStr;
use super::aggregate::{self, Aggregate, AggregateParseError};

/// Instances of this struct define columns with aggregated values to be added
/// to each row of the stream.
///
/// The best method for creating an AggregatedCol is by parsing a string with
/// the spec of the new column,
#[derive(Clone)]
pub struct AggregatedCol {
    colname: String,
    aggregate: Box<dyn Aggregate>,
}

impl AggregatedCol {
    /// Manually create an AggregatedCol by its ingredients which are the
    /// new column's name, the soure column's name and an implementor of the
    /// [Aggregate] trait.
    pub fn new(colname: &str, aggregate: Box<dyn Aggregate>) -> AggregatedCol {
        AggregatedCol {
            colname: colname.to_string(),
            aggregate,
        }
    }

    /// Get this aggregate's colname
    pub fn colname(&self) -> &str {
        &self.colname
    }

    /// Get this aggregate's Aggregate
    pub fn aggregate(&self) -> &Box<dyn Aggregate> {
        &self.aggregate
    }
}

impl FromStr for AggregatedCol {
    type Err = AggregateParseError;

    fn from_str(def: &str) -> Result<AggregatedCol, Self::Err> {
        let pieces: Vec<&str> = def.split(':').collect();

        if pieces.len() < 2 {
            return Err(AggregateParseError::TooFewParts);
        }

        Ok(AggregatedCol {
            colname: pieces[0].to_string(),
            aggregate: match pieces[1] {
                "avg" => Box::new(aggregate::Avg::new(&pieces[2..])?),
                "count" => Box::new(aggregate::Count::new(&pieces[2..])?),
                "last" => Box::new(aggregate::Last::new(&pieces[2..])?),
                "max" => Box::new(aggregate::Max::new(&pieces[2..])?),
                "defaultmax" => Box::new(aggregate::DefaultMax::new(&pieces[2..])?),
                "min" => Box::new(aggregate::Min::new(&pieces[2..])?),
                "defaultmin" => Box::new(aggregate::DefaultMin::new(&pieces[2..])?),
                "sum" => Box::new(aggregate::Sum::new(&pieces[2..])?),
                s => return Err(AggregateParseError::UnknownAggregate(s.to_string())),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::AggregatedCol;

    #[test]
    fn test_parse_avg() {
        let col: AggregatedCol = "newcol:avg:prev".parse().unwrap();

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "NaN");
    }

    #[test]
    fn test_parse_cout() {
        let col: AggregatedCol = "newcol:count".parse().unwrap();

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "0");
    }

    #[test]
    fn test_parse_last() {
        let col: AggregatedCol = "newcol:last:prev".parse().unwrap();

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "");
    }

    #[test]
    fn test_parse_max() {
        let col: AggregatedCol = "newcol:max:prev".parse().unwrap();

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "-inf");
    }

    #[test]
    fn test_parse_min() {
        let col: AggregatedCol = "newcol:min:prev".parse().unwrap();

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "inf");
    }

    #[test]
    fn test_parse_sum() {
        let col: AggregatedCol = "newcol:sum:prev".parse().unwrap();

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "0");
    }

    #[test]
    fn test_parse_defaultmax() {
        let col: AggregatedCol = "newcol:defaultmax:prev".parse().unwrap();

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "-inf");
    }

    #[test]
    fn test_parse_defaultmin() {
        let col: AggregatedCol = "newcol:defaultmin:prev".parse().unwrap();

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "inf");
    }
}
