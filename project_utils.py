import os
import subprocess
import re
import json
import uuid
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
    
    if "where" in query:
        start_where = re.search("where", query).start()
        table = query[endCols + 4: start_where - 1].strip()
    else:
        table = query[endCols + 4:].strip()
    schemaFile = f"/hive_test/{table.split('/')[0]}/schema_{table.split('/')[1]}.json"
    check = misc_utils.isFileExists(schemaFile)

    if check:
        
        colIndexes = []
        ufolder = "schema_" + str(uuid.uuid4().hex)
        os.system(f"mkdir ./{ufolder}")
        cmd = f"hadoop fs -get {schemaFile} ./{ufolder}"
        os.system(cmd)

        with open(f"./{ufolder}/schema_{table.split('/')[1]}.json", "r") as f:
            schema = json.load(f)

        colList = projectCols.split(',')

        if colList[0].strip() == '*':
            for i in schema.values():
                colIndexes.append(i[0])

        else:
            for col in colList:
                if col.strip() not in schema:
                    print("Invalid column name")
                    return
                else:
                    colIndexes.append(schema[col.strip()][0])


        m_filename = "mapper_" + str(uuid.uuid4().hex) + ".py"
        mapper = open(m_filename, "w")
        if(len(re.findall("where", query)) == 1):
            # Have to parse query to get condition

            start_cond = re.search("where", query).start() + 5
            condition = query[start_cond:]
            for valid_col, col_data in schema.items():
                if valid_col in condition:
                    if col_data[1] == "int":
                        condition = condition.replace(valid_col, f"int(rowValues[{col_data[0]}])")
                    else:
                        condition = condition.replace(valid_col, f"str(rowValues[{col_data[0]}])")
            
            condition = condition.replace("<=", "<*")
            condition = condition.replace(">=", ">*")
            condition = condition.replace("!=", "!*")
            condition = condition.replace("=", "==")
            condition = condition.replace("*", "=")

            misc_utils.write_map_select(colIndexes, condition.strip(), mapper)

        elif(len(re.findall("where", query)) == 0):
            misc_utils.write_map_project(colIndexes, mapper)

        else:
            print("Command unrecognizable")
            return

        r_filename = "reducer_" + str(uuid.uuid4().hex) + ".py"
        reducer = open(r_filename, "w")
        misc_utils.write_reducer(code, reducer, projectCols)

        outputdir = "output" + str(uuid.uuid4().hex)

        runcmd = f"hadoop jar /home/hduser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -mapper {m_filename} -reducer {r_filename} -input /hive_test/{table.split('/')[0]}/input -output /hive_test/{table.split('/')[0]}/{outputdir}"

        os.system(runcmd)

        displaycmd = f"hadoop fs -cat /hive_test/{table.split('/')[0]}/{outputdir}/part-00000"

        os.system(displaycmd)

        rm_rm = f"rm -f {m_filename} {r_filename}"
        rm_schema = f"rm -rf ./{ufolder}"
        rm1 = f"hadoop fs -rm -r /hive_test/{table.split('/')[0]}/{outputdir}"
        os.system(rm_rm)
        os.system(rm_schema)
        os.system(rm1)


    else:
        print("Table does not exist")


    


