# datafusion-functions-extra

[![CI](https://github.com/datafusion-contrib/datafusion-functions-extra/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/datafusion-contrib/datafusion-functions-extra/actions/workflows/ci.yml?query=branch%3Amain)

[![Crates.io](https://img.shields.io/crates/v/datafusion-functions-extra?color=green)](https://crates.io/crates/datafusion-functions-extra)

**Note:** This is not an official Apache Software Foundation release.

This crate provides extra functions for DataFusion, specifically focusing on advanced aggregations. These extensions are inspired by other projects like DuckDB and Spark SQL.

To use these functions, you'll just need to call:

```rust
datafusion_functions_extra::register_all_extra_functions(&mut ctx)?;
```

# Examples

```sql
-- Create a table with various columns containing strings, integers, floats, dates, and times
CREATE TABLE test_table (
    utf8_col VARCHAR,
    int64_col INT,
    float64_col FLOAT,
    date64_col DATE,
    time64_col TIME
) AS VALUES
('apple', 1, 1.0, '2021-01-01', '01:00:00'),
('banana', 2, 2.0, '2021-01-02', '02:00:00'),
('apple', 2, 2.0, '2021-01-02', '02:00:00'),
('orange', 3, 3.0, '2021-01-03', '03:00:00'),
('banana', 3, 3.0, '2021-01-03', '03:00:00'),
('apple', 3, 3.0, '2021-01-03', '03:00:00');

-- Get the mode of the utf8_col column
SELECT mode(utf8_col) as mode_utf8 FROM test_table;
-- Results in
-- +----------+
-- | mode_utf8|
-- +----------+
-- | apple    |
-- +----------+

-- Get the mode of the date64_col column
SELECT mode(date64_col) as mode_date FROM test_table;
-- Results in
-- +-----------+
-- | mode_date |
-- +-----------+
-- | 2021-01-03|
-- +-----------+

-- Get the mode of the time64_col column
SELECT mode(time64_col) as mode_time FROM test_table;
-- Results in
-- +-----------+
-- | mode_time |
-- +-----------+
-- | 03:00:00  |
-- +-----------+

-- Get the x value associated with the maximum y value
SELECT max_by(x, y) FROM VALUES (1, 10), (2, 5), (3, 15), (4, 8) as tab(x, y);
-- Results in
-- +---------------------+
-- | max_by(tab.x,tab.y) |
-- +---------------------+
-- | 3                   |
-- +---------------------+

-- Get the x value associated with the minimum y value
SELECT min_by(x, y) FROM VALUES (1, 10), (2, 5), (3, 15), (4, 8) as tab(x, y);
-- Results in
-- +---------------------+
-- | min_by(tab.x,tab.y) |
-- +---------------------+
-- | 2                   |
-- +---------------------+

```

## Done

- [x] `mode(expression) -> scalar` - Returns the most frequent (mode) value from a column of data.
- [x] `max_by(expression1, expression2) -> scalar` - Returns the value of `expression1` associated with the maximum value of `expression2`.
- [x] `min_by(expression1, expression2) -> scalar` - Returns the value of `expression1` associated with the minimum value of `expression2`.
- [x] `skewness(expression) -> scalar` - Computes the skewness value for `expression`.
- [x] `kurtois_pop(expression) -> scalar` - Computes the excess kurtosis (Fisher’s definition) without bias correction.
- [x] `kurtosis(expression) -> scalar` - Computes the excess kurtosis (Fisher’s definition) with bias correction according to the sample size.
