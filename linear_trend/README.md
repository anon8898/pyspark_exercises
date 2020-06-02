#PySpark Exercises

**linear trend example**

##Data

If working from this directory, the data can be imported with the following command

```python
from data import generator

## the number of lines chosen is optional, but needs to be greater than 100
data = generator.basic_linear(lines = 1000)
```

Alternatively, the data can be run from the generator script in the data folder, and piped into a text file for a static import

##Questions

**Question One**

Assume the underlying data shows a linear trend, using a simplistic model (y = mx + b) to predict the null values in the dataset

As a note, the null values, regardless of the number of lines generated, will be the last 10 lines of a dataset - feel free to change this by changing the code if you rather predict more than 10 data points

