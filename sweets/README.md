# PySpark Exercises

**sweets trend example**

## Data

If working from this directory, the data can be imported with the following command

```python
from data import generator

## the number of lines chosen is optional, but needs to be greater than 1000
data = generator.sweets(lines = 1000)
```

Alternatively, the data can be run from the generator script in the data folder, and piped into a text file for a static import

## Questions

**Question One**

Which sweet raised the most money?

**Question Two**

The location columns (one, two three) are how the funds for a sweet and customer is distributed after purchase, find a table on the breakdown of funds for each area vs sweets

*Hint*
```
+-------+------------+------------+--------------+
|sweets |location_one|location_two|location_three|
+-------+------------+------------+--------------+
|sweet1 |100         |166         |878           |
|sweet2 |200         |321         |690           |
|sweet3 |150         |420         |111           |
+-------+------------+------------+--------------+
```

**Question Three**

Find all instances of velvet flavoured sweets and count which one has the most instances
