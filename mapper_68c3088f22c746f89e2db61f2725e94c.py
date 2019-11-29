#!/usr/bin/python3
import sys
infile = sys.stdin
for line in infile:
	line = line.strip()
	rowValues = line.split(',')
	if(int(rowValues[2]) == 90):
		print(rowValues[1])
