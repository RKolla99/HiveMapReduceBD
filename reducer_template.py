#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

var1 = 0
var2 = 0

for line in sys.stdin:
    line = line.strip()
    line_val = line.split(',')
  
