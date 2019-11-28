import os
import subprocess


def isDbExists(path):
    
    cmd = f"hadoop fs -test -d {path};echo $?"
    check = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE).communicate()

    if '1' not in str(check):
        return 1
    else:
        return 0

def isFileExists(path):

    cmd = f"hadoop fs -test -f {path};echo $?"
    check = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE).communicate()

    if '1' not in str(check):
        return 1
    else:
        return 0

def write_map_select(indexList, condition, mapper):

    mapper.write("#!/usr/bin/python3\n")
    mapper.write("import sys\n")
    mapper.write("infile = sys.stdin\n")
    mapper.write("for line in infile:\n")
    mapper.write("\tline = line.strip()\n")
    mapper.write("\trowValues = line.split(',')\n")
    mapper.write(f"\tif({condition}):\n")
    mapper.write("\t\tprint(")
    for index in range(len(indexList) - 1):        
        mapper.write(f"rowValues[{indexList[index]}], ',' ,")
    mapper.write(f"rowValues[{indexList[len(indexList) - 1]}])")
    mapper.close()

def write_map_project(indexList, mapper):

    mapper.write("#!/usr/bin/python3\n")
    mapper.write("import sys\n")
    mapper.write("infile = sys.stdin\n")
    mapper.write("for line in infile:\n")
    mapper.write("\tline = line.strip()\n")
    mapper.write("\trowValues = line.split(',')\n")
    mapper.write("\tprint(")
    for index in range(len(indexList) - 1):        
        mapper.write(f"rowValues[{indexList[index]}], ',' ,")
    mapper.write(f"rowValues[{indexList[len(indexList) - 1]}])")
    mapper.close()

def write_red_identity(reducer):

    reducer.write("\tprint(line)")    
    reducer.close()
    
def write_red_max(reducer):
    
    reducer.write("\tline = line.strip()\n")
    reducer.write("\tif var == None:\n")
    reducer.write("\t\tvar = line\n")
    reducer.write("\telse:\n")
    reducer.write("\t\tif line > var:\n")
    reducer.write("\t\t\tvar = line\n")
    reducer.write("print(var)")
    reducer.close()

def write_red_min(reducer):
    
    reducer.write("\tline = line.strip()\n")
    reducer.write("\tif var == None:\n")
    reducer.write("\t\tvar = line\n")
    reducer.write("\telse:\n")
    reducer.write("\t\tif line < var:\n")
    reducer.write("\t\t\tvar = line\n")
    reducer.write("print(var)")
    reducer.close()

def write_red_count(reducer):

    reducer.write("\tline = line.strip()\n")
    reducer.write("\tcount = count + 1\n")
    reducer.write("print(count)")
    reducer.close()

def write_reducer(code, reducer):

    reducer.write("#!/usr/bin/python3\n")
    reducer.write("import sys\n")
    reducer.write("var = None\n")
    reducer.write("count = 0\n")
    reducer.write("for line in sys.stdin:\n")

    if code == 0:
        write_red_identity(reducer)
    elif code == 1:
        write_red_max(reducer)
    elif code == 2:
        write_red_min(reducer)
    else:
        write_red_count(reducer)
