use super::aggregate::Aggregate;

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

#[cfg(test)]
mod tests {
    use super::AggregatedCol;
    use crate::aggregate::{
        Avg, Min, Max, DefaultMin, DefaultMax, Count, Last, Sum,
    };

    #[test]
    fn test_parse_avg() {
        let col = AggregatedCol::new("newcol", Box::new(Avg::new("prev")));

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "NaN");
    }

    #[test]
    fn test_parse_cout() {
        let col = AggregatedCol::new("newcol", Box::new(Count::new()));

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "0");
    }

    #[test]
    fn test_parse_last() {
        let col = AggregatedCol::new("newcol", Box::new(Last::new("prev")));

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "");
    }

    #[test]
    fn test_parse_max() {
        let col = AggregatedCol::new("newcol", Box::new(Max::new("prev")));

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "-inf");
    }

    #[test]
    fn test_parse_min() {
        let col = AggregatedCol::new("newcol", Box::new(Min::new("prev")));

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "inf");
    }

    #[test]
    fn test_parse_sum() {
        let col = AggregatedCol::new("newcol", Box::new(Sum::new("prev")));

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "0");
    }

    #[test]
    fn test_parse_defaultmax() {
        let col = AggregatedCol::new("newcol", Box::new(DefaultMax::new("prev")));

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "-inf");
    }

    #[test]
    fn test_parse_defaultmin() {
        let col = AggregatedCol::new("newcol", Box::new(DefaultMin::new("prev")));

        assert_eq!(col.colname(), "newcol");
        assert_eq!(col.aggregate().value(), "inf");
    }
}
