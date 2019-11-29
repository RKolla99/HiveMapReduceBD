#!/usr/bin/python3
import sys
infile = sys.stdin
for line in infile:
	line = line.strip()
	rowValues = line.split(',')
	print(rowValues[1])