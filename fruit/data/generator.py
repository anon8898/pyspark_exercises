#!/usr/bin/env python3.7

from random import uniform, choices, sample

def choosing_bounds(lines):
    upper_bound = lines * 10
    lower_bound = 10 ** (len([x for x in str(upper_bound)]) - 1)
    if upper_bound == lower_bound:
        upper_bound = (lines * 10) - 1
        lower_bound = 10 ** (len([x for x in str(lines * 10)]) - 2)
    return [upper_bound, lower_bound]

def fruit(lines = 1000):
    if lines < 10:
        print("lines value should be at least 10")
        lines = 10
    upper_bound, lower_bound = choosing_bounds(lines)
    options = ['apple', 'banana', 'orange', 'pear', 'cherry']
    customers = [ sample(range(lower_bound, upper_bound), k = 1)[0] for x in range(1, lines) ]
    num_choices = [ uniform(1,5) for x in range(1,lines) ]
    customer_choices = [ ",".join(choices(options, k = int(x))) for x in num_choices ]
    age = [ int(uniform(18,55)) for x in customers ]
    return zip(customers, age, customer_choices)


