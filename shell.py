import os
import subprocess
import json
import re
import select_utils
import project_utils
import misc_utils

while(True):
    query = input("shell> ").lower()
    
    query_list = query.split()

    if(query_list[0] == "exit"):
        break

    elif(query_list[0] == "create" and query_list[1] == "db"):
        # Check if database already exists
        check = misc_utils.isDbExists(f"/hive_test/{query_list[2]}")

        if not check:
            # Create directory on HDFS
            cmd = f"hadoop fs -mkdir -p /hive_test/{query_list[2]}"
            os.system(cmd)
        else:
            print(f"{query_list[2]} already exists")
        

    elif(query_list[0] == "load" and query_list[2] == "as"):

        # Path to the database
        path = f"/hive_test/{query_list[1]}"
        table = query_list[1].split('/')[1]
        db = query_list[1].split('/')[0]
        check = misc_utils.isFileExists(path)

        if path:
            # Dictionary and a temporary file for storing the schema before
            # putting it onto HDFS
            schemaDict = {}
            schemaFile = open(f"schema_{table}.json", "w+")
            schemaStr = ""

            for i in range(3, len(query_list)):
                schemaStr += query_list[i]
            schemaList = schemaStr.split(",")
            
            for i in range(len(schemaList)):
                name, datatype = schemaList[i].split(":")
                schemaDict[name] = [i, datatype]

            # Jsonify the dictionary and dump it onto the file
            jsonDict = json.dumps(schemaDict)
            schemaFile.write(jsonDict)
            schemaFile.close()

            # Put the schema onto HDFS and remove the temporary file
            cmd = f"hadoop fs -put ./schema_{table}.json /hive_test/{db}/"
            os.system(cmd)
            os.system(f"rm -f ./schema_{table}.json")        
        else:
            print("File does not exist")

    elif(query_list[0] == "select"):
        if(len(re.findall("where", query)) == 1):
            select_utils.run(query)
        else:
            project_utils.run(query)

    else:
        print("Command unrecognizable")
