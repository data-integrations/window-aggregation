# GroupBy Aggregate


Description
-----------
Specify a window over which functions should be applied.
Supported functions: `Rank`, `Dense Rank`, `Percent Rank`, `N tile`, `Row Number`, `Median`, `Continuous Percentile`, `Lead`, `Lag`, `First`, `Last`, `Cumulative distribution`, `Accumulate`.

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
Sorts numerically for numeric order by field, lexicographically for strings.
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
`Dense Rank`, `Percent Rank`, `N tile`, `Row Number`, `Median`, `Continuous Percentile`, `Lead`, 
`Lag`, `First`, `Last`, `Cumulative distribution`, `Accumulate`. Aggregates are specified using syntax: 
.`alias:function(field,encoded(arguments),ignoreNulls)[\n other functions]`. For example, 
`nextValue:lead(value,1,false)\npreviousValue:lag(value,1,false)` will calculate two aggregates. The first will create a
field called `nextValue` that is the next value of current row in the group. The second will create a field called
`previousValue` that is the previous value of current row in the group.

**Number of partitions:** Number of partitions to use when grouping fields. If not specified, the execution
framework will decide on the number to use.
