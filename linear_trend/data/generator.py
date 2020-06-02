#!/usr/bin/env python3

from random import uniform, random

def basic_linear(lines = 1000):
    if int(lines) < 100:
        lines = 100
    else:
        lines = int(lines)
    x_values = [ x for x in range(1,lines+1) ]
    cut_off_x = x_values[-10:]
    y_values = [ round(x + uniform(-2,2), 2) for x in x_values if x not in cut_off_x ] + [ None for x in cut_off_x ]
    return zip(x_values, y_values)

