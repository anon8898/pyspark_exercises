#!/usr/bin/env python3

from random import uniform, choices, sample
from datetime import datetime, date

def choosing_bounds(lines):
    upper_bound = max(lines, 999999)
    lower_bound = 10 ** (len([x for x in str(upper_bound)]) - 1)
    if upper_bound == lower_bound:
        upper_bound = (lines * 10) - 1
        lower_bound = 10 ** (len([x for x in str(lines * 10)]) - 2)
    return [upper_bound, lower_bound]

def finance(lines = 1000):
    lines = int(lines)
    customer_id = [ "{:0{}d}".format(x, len(str(lines))) for x in range(1,lines) ]
    dates = [ date.fromtimestamp(315532800 + int(uniform(100000, 999999))) for x in customer_id ]
    transactions = [ "${}".format(round(uniform(10,500), 2)) for x in customer_id ]
    companies = ["XYZ ltd", "Pear Inc.", "Mouse Co.", "ABC pty", "Lamp Co.", "J&K Inc.", "Blue Inc."]
    company = [ choices(companies, k = 1) for x in customer_id]
    return zip(customer_id, dates, transactions, company)

if __name__ == "__main__":
    for x in finance():
        print(x)
