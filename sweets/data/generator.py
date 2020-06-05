#!/usr/bin/env python3

from random import sample, choices, randint

## global

data_flavours = ["chocolate", "vanilla", "cherry", "banana", "butterscotch", "apple", "mango", "fruit", "velvet", "red velvet", "orange velvet", "pinapple", "caramel", "lemon", "apricot", "rum", "strawberry", "raisin", "cinnamon"]
data_pastries = ["cake", "cupcakes", "cheesecake", "pastry", "puffs", "slice", "pie", "crumble"]

def choosing_bounds(lines):
    upper_bound = max(lines, 999999)
    lower_bound = 10 ** (len([x for x in str(upper_bound)]) - 1)
    if upper_bound == lower_bound:
        upper_bound = (lines * 10) - 1
        lower_bound = 10 ** (len([x for x in str(lines * 10)]) - 2)
    return [upper_bound, lower_bound]

## ..some weird results will come from this
def sweets(lines = 1000):
    if int(lines) < 1000:
        lines = 1000
    else:
        lines = int(lines)
    upper_bound, lower_bound = choosing_bounds(lines)
    customers = sample(range(lower_bound, upper_bound), k = lines)
    chosen_sweet = [ "{} {}".format(choices(data_flavours, k=1)[0], choices(data_pastries, k=1)[0]) for x in customers ]
    random_num = [ choices(range(1,10),k=3) for x in customers ]
    random_perc = [ (round(x[0]/sum(x),2), round(x[1]/sum(x),2), round(x[2]/sum(x),2)) for x in random_num ]
    raised_fund = [ "${}.00".format(randint(1000,2000)) for x in customers ]
    ## data header: customer_id, sweets, raised_funds, location_one, location_two, location_three
    return zip(customers, chosen_sweet, raised_fund, [x[0] for x in random_perc], [x[1] for x in random_perc], [x[2] for x in random_perc])

