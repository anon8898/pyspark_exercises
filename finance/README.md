# Pyspark Exercises
**finance examples**

## Data

The data for these exercises is generated via script.

It can be imported via the following code snippet

```python
from data import generator

## default number of lines is 1000, can choose whatever value if you'd like to work with bigger data
data = generator.finance(lines = 1000)
```

Alternatively, the data can be run directly into a text output to be used from a static file

*Example of the data:*

```
# customer_id, date, purchase_cost, company
('0997', 1980-01-11, '$314.2', ['Mouse Co.'])
('0998', 1980-01-12, '$447.74', ['Mouse Co.'])
('0999', 1980-04-10, '$337.22', ['Lamp Co.'])
```

## Questions

**Question One**

Put the data into a dataframe where the schema for the columns is string, date, float, string**

*potential format*
```
root
 |-- customer_id: string (nullable = true)                                                               
 |-- date: date (nullable = true)
 |-- purchase_cost: float (nullable = true)
 |-- company: string (nullable = true)
```

**Question Two**

Aggregate the 'purchase_costs' column to find the most profitable company

**Question Three**

Which date is the most profitable per company

