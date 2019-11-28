import os
import subprocess
import re
import json
import misc_utils

def run(query):

    endCols = re.search("from", query).start()
    projectCols = query[6:endCols].strip()
    
    code = 0

    if(("max(" in projectCols) or ("min(" in projectCols)):
        if("max(" in projectCols):
            code = 1
        else:
            code = 2
        projectCols = projectCols[4:].strip(")")
    if("count(" in projectCols):
        code = 3
        projectCols = projectCols[6:].strip(")")
    
    table = query[endCols + 4:].strip()
    schemaFile = f"/hive_test/{table.split('/')[0]}/schema_{table.split('/')[1]}.json"
    check = misc_utils.isFileExists(schemaFile)

    if check:
        
        colIndexes = []
        cmd = f"hadoop fs -get {schemaFile} ."
        os.system(cmd)

        with open(f"schema_{table.split('/')[1]}.json", "r") as f:
            schema = json.load(f)

        colList = projectCols.split(',')

        if colList[0].strip() == '*':
            for i in schema.values():
                colIndexes.append(i[0])

        else:
            for col in colList:
                if col.strip() not in schema:
                    print("Invalid column name")
                    exit()
                else:
                    colIndexes.append(schema[col.strip()][0])

        mapper = open("mapper.py", "w")
        misc_utils.write_mapper(colIndexes, mapper)

        reducer = open("reducer.py", "w")
        misc_utils.write_reducer(code, reducer)

        runcmd = f"hadoop jar /home/hduser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -mapper mapper.py -reducer reducer.py -input /hive_test/{table.split('/')[0]}/input -output /hive_test/{table.split('/')[0]}/output"

        os.system(runcmd)

        displaycmd = f"hadoop fs -cat /hive_test/{table.split('/')[0]}/output/part-00000"

        os.system(displaycmd)

        rm = f"rm -f mapper.py reducer.py schema_{table.split('/')[1]}.json"
        rm1 = f"hadoop fs -rm -r /hive_test/{table.split('/')[0]}/output"
        os.system(rm)
        os.system(rm1)


    else:
        print("Table does not exist")


    


