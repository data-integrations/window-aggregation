# Window Aggregation


Description
-----------
Specify a window over which functions should be applied.
Supported functions: `Rank`, `Dense Rank`, `Percent Rank`, `N tile`, `Row Number`, `Median`, `Continuous Percentile`, `Lead`, `Lag`, `First`, `Last`, `Cumulative distribution`, `Accumulate`.

BigQuery ELT Transformation Pushdown
-----------
Window aggregation stages are now eligible to execute in BigQuery when BigQuery ELT Transformation Pushdown is enabled in a pipeline. Window aggregation stages will be executed in BigQuery when a preceding stage has already been executed in BigQuery (such as a Join operation or another aggregation stage) or if the source is BigQuery. All the above mentioned functions are supported in BigQuery. 

Use Case
--------
The transform is used when you want to calculate some basic aggregations in your data similar
to what you could do with a window function in SQL.

Properties
----------
**Partition fields:** Specifies a list of fields, comma-separated, to partition the data by. 
At least 1 field must be provided.
Records with the same value for all these fields will be grouped together.

**Order:** Specifies key-value pairs containing the ordering field, and the order (ascending or descending).
All data types are allowed, except when `Frame Type` is RANGE and `Unbounded preceding` or `Unbounded following`
is set to `false`, order must be single expression and data type must be one of: `Int`, `Long`, `Double`, `Float`
For example: `value:Ascending,id:Descending`.

**Frame Type:** Selects the type of window frame to create within each partition. Options can be `ROW`, `RANGE`
 or `NONE`. Default is `NONE`. 

**Unbounded preceding:** Whether to use an unbounded start boundary for a frame. Default is `false`.

**Unbounded following:** Whether to use an unbounded end boundary for a frame. Default is `false`.

**Preceding:** Specifies the number of preceding rows in the window frame. When Frame Type is `ROW`, this is a number
relative to the current row. E.g. -2 to begin the frame 2 rows before the current row. When Frame Type is `RANGE`, 
specifies the value to be subtracted from the value of the current row to get the start boundary.

**Following:** Specifies the number of following rows to include in the window frame. When Frame Type is `ROW`, this is 
a number relative to the current row. E.g. 3 to end the frame 3 rows after the current row. When Frame Type is `RANGE`, 
specifies the value to be added to the value of the current row to get the end boundary.


**Aggregates:** Specifies a list of functions to run on the selected window. Supported aggregate functions are `Rank`, 
`Dense_Rank`, `Percent_Rank`, `N_Tile`, `Row_Number`, `Median`, `Continuous_Percentile`, `Lead`, 
`Lag`, `First`, `Last`, `Cumulative_Distribution`, `Accumulate`. Aggregates are specified using syntax: 
.`alias:function(field,encoded(arguments),ignoreNulls)[\n other functions]`. For example, 
`nextValue:lead(value,1,false)\npreviousValue:lag(value,1,false)` will calculate two aggregates. The first will create a
field called `nextValue` that is the next value of current row in the group. The second will create a field called
`previousValue` that is the previous value of current row in the group.

**Number of partitions:** Number of partitions to use when grouping fields. If not specified, the execution
framework will decide on the number to use.

----------
**Clause Constraints:**

| Function | Partition fields | Order | Frame Type | 
| :------------ | :------: | :----- | :---------- |
| rank                    | required  | required      | not supported |
| dense_rank              | required  | required      | not supported |
| percent_rank            | required  | required      | not supported |
| n_tile                  | required  | required      | not supported |
| row_number              | required  | required      | not supported |
| continous_percentile    | required  | not supported | not supported |
| lead                    | required  | required      | not supported |
| lag                     | required  | required      | not supported |
| first                   | required  | required      | optional      |
| last                    | required  | required      | optional      |
| cumulative_distribution | required  | required      | not supported |
| accumulate              | required  | optional      | optional      |

**Functions with Arguments:**
There are few functions which require the `field` and `argument` as per the syntax `alias:function(field,encoded(arguments),ignoreNulls)`. If the function doesn't require the field or the argument, then it's ignored. 

| Function | field  | argument  |
| :------------ | :------: | :----- |
| rank                    |   |       |
| dense_rank              |   |       | 
| percent_rank            |   |       | 
| n_tile                  |   | required : an integer greater than 0     | 
| row_number              |   |       | 
| continous_percentile    | required  | required : a numeric between 0 and 1 (both inclusive) |
| lead                    | required  | required : a non-negative integer     | 
| lag                     | required  | required : a non-negative integer      | 
| first                   | required  |       | 
| last                    | required  |       | 
| cumulative_distribution |   |       | 
| accumulate              | required  |       | 


Sample Pipeline Data
----------
**Input Records**

|name  |age|location|
|------|---|--------|
|peter |20 |US      |
|foo   |22 |US      |
|rajeev|24 |US      |
|john  |28 |US      |
|alex  |30 |US      |
|ravi  |20 |INDIA   |
|kenny |30 |INDIA   |

**Window Aggregations Config**

Partition fields : `location`

Order : `age:ascending`

Frame Type : `None`

Aggregates :

`my_rank: rank(,,true)`

`next_value:lead(age,1,false)`

**Output Records**

|name  |age|location|my_rank|next_value|
|------|---|--------|-------|----------|
|peter |20 |US      |1      |22        |
|foo   |22 |US      |2      |24        |
|rajeev|24 |US      |3      |28        |
|john  |28 |US      |4      |30        |
|alex  |30 |US      |5      |          |
|ravi  |20 |INDIA   |1      |30        |
|kenny |30 |INDIA   |2      |          |

