# Pyspark Exercises
**fruit examples**

## Data

The data for these exercises is generated via script.

It can be imported via the following code snippet

```python
from data import generator

## default number of lines is 1000, can choose whatever value if you'd like to work with bigger data
data = generator.fruit(lines = 1000)
```

Alternatively, the data can be run directly into a text output to be used from a static file

*Example of the data:*

```
# zip of lists

# (customer_id, age, list_of_fruits_purchased)
(10, 28, 'banana,orange,apple')
(21, 40, 'apple')
(94, 39, 'orange')
(74, 39, 'pear')
(71, 29, 'banana,banana')
(73, 41, 'cherry,cherry')
(87, 28, 'pear,apple,pear')
(47, 25, 'cherry,orange,pear,pear')
(14, 21, 'cherry')
```

## Questions

**Questions One**

Count how many apples are bought per customer.

Order the results in descending order by the customer that bought the most apples, then by customer id (in any order)

**Question Two**

Count how many pears are bought by customers, split by consumers above the age of 30, and below the age of 30

**Question Three**

Count how many fruits are consumed per person, each fruit needs to be a new column

*example of potential output*

```
+-----------+----------+---------+----------+--------+----------+
|customer_id|sum_orange|sum_apple|sum_cherry|sum_pear|sum_banana|
+-----------+----------+---------+----------+--------+----------+
|       1224|         1|        0|         0|       1|         0|
|       1840|         0|        1|         0|       0|         0|
|       3009|         2|        1|         0|       0|         0|
+-----------+----------+---------+----------+--------+----------+
```



