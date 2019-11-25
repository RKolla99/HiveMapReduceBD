import os
import subprocess

dbname = ""
while(True):
    query = input("shell>").lower()
    
    query_list = query.split()

    if(query_list[0] == "exit"):
        break

    elif(query_list[0] == "create" and query_list[1] == "db"):
        cmd = f"hadoop fs -mkdir -p /hive_test/{query_list[2]}"
        os.system(cmd)

    elif(query_list[0] == "load" and query_list[2] == "as"):
        if dbname == query_list[1].split('/')[0]:
            path = f"/hive_test/{query_list[1]}"
            schemaStr = ""
            for i in range(3, len(query_list)):
                schemaStr += query_list[i]
            schemaList = schemaStr.split(",")
            schemaFile = open(f"{query_list[1].split('/')[1]}_schema.txt", "w+")
            for i in schemaList:
                name, datatype = i.split(":")
                schemaFile.write("%s:%s\n" % (name, datatype))
            schemaFile.close()
            cmd = f"hadoop fs -put ./{query_list[1].split('/')[1]}_schema.txt /hive_test/{query_list[1].split('/')[0]}/"
            os.system(cmd)


    elif(query_list[0] == "use"):
        cmd = f"hadoop fs -test -d /hive_test/{query_list[1]};echo $?"
        check = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE).communicate()
        if '1' not in str(check):
            dbname = query_list[1]
        else:
            dbname = ""
            print(f"{query_list[1]} is not a database")

    else:
        print("Command unrecognizable")
