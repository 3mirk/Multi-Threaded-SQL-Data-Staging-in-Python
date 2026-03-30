import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import mysql.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from threading import Lock
import time
import os

# concurrent.futures is a built-in Python library
# TPE: allows parallel thread processing
# as_completed: iterate over results as soon as each task is finished regardless of order

# List of all SQL Connections 
sn_list = [
    {"Lab": "xxx", "IP": "x.x.x.x", "SN": "123", "Mapper": "v2", "SQL": "PostgreSQL", "Username": "as", "Password": "as"},
    {"Lab": "xxx", "IP": "x.x.x.x", "SN": "134", "Mapper": "v3", "SQL": "MySQL", "Username": "as", "Password": "as"},
    ...
]

# SQL query for PostgreSQL Databases
def get_postgres_query(sn):
    return f"""
        SELECT
            results."CreatedDate", results."Workorder" AS "Tray", results."Eye", results."LNAM", results."LIND",
            results."TINT" AS "TINT", results."POLAR" AS "Polar", results."PRAX_MES" AS "PRAX MES",
            results."TolStd", "TOLSET",
            results."ManuelPositionned" AS "ManualPPOS", results."ManuelModeReason" AS "ManualPPOSReason",
            results."LTYPE", results."LTYPEF", results."LTYPEB",
            results."LDSPH" AS "Sph_Nom", results."SPH_MES" AS "Sph_Mes", results."SPH_Max" AS "Sph_Tol", results."SPH_InTol" AS "Sph_Ver",
            results."LDCYL" AS "Cyl_Nom", results."CYL_MES" AS "Cyl_Mes", results."CYL_Max" AS "Cyl_Tol", results."CYL_InTol" AS "Cyl_Ver",
            results."LDAX" AS "Ax_Nom", results."AX_MES" AS "Ax_Mes", results."AX_Max" AS "Ax_Tol", results."AX_InTol" AS "Ax_Ver",
            results."LDPRVM" AS "PD_Nom", results."PRVM_MES" AS "PD_Mes", results."PRVM_Max" AS "PD_Tol", results."PRVM_Min" AS "PD_Min", results."PRVM_InTol" AS "PD_Ver",
            results."LDPRVA" AS "PA_Nom", results."PRVA_MES" AS "PA_Mes", results."PRVA_Max" AS "PA_Tol", results."PRVA_Min" AS "PA_Min", results."PRVA_InTol" AS "PA_Ver",
            results."LDADD" AS "Add_Nom", results."ADD_MES" AS "Add_Mes", results."ADD_Max" AS "Add_Tol", results."ADD_InTol" AS "Add_Ver",
            results."LDCTHK" AS "Thk_Nom", results."CTHICK_MES" AS "Thk_Mes", results."CTHICK_Max" AS "Thk_Tol", results."CTHICK_Min" AS "Thk_Min", results."CTHICK_InTol" AS "Thk_Ver",
            results."LDPRVH" AS "PH_Nom", results."PRVH_MES" AS "PH_Mes", results."PRVH_Max" AS "PH_Tol", results."PRVH_Min" AS "PH_Min", results."PRVH_InTol" AS "PH_Ver",
            results."LDPRVV" AS "PV_Nom", results."PRVV_Mes" AS "PV_Mes", results."PRVV_Max" AS "PV_Tol", results."PRVV_Min" AS "PV_Min", results."PRVV_InTol" AS "PV_Ver",
            results."GripperNum" AS "Gripper", 
            results."ErrorId" AS "Status", results."ErrorMsg" AS "Error", results."ErrorGroupId" AS "Category"
        FROM results
        ORDER BY "CreatedDate" DESC
        LIMIT 75000;
    """

# SQL query for MySQL Databases
def get_mysql_query(sn):
    return f"""
        SELECT results.`CreatedDate`, results.`Workorder` AS `Tray`, results.`Eye`, results.`LNAM`, results.`LIND`,
            results.`TINT`, results.`POLAR`, results.`PRAX_MES`,
            results.`TolStd`, results.`TOLSET`,
            results.`ManuelPositionned` AS `ManualPPOS`, results.`ManuelModeReason` AS `ManualPPOSReason`,
            results.`LTYPE`, results.`LTYPEF`, results.`LTYPEB`,
            results.`LDSPH` AS `Sph_Nom`, results.`SPH_MES` AS `Sph_Mes`, results.`SPH_Max` AS `Sph_Tol`, results.`SPH_InTol` AS `Sph_Ver`,
            results.`LDCYL` AS `Cyl_Nom`, results.`CYL_MES` AS `Cyl_Mes`, results.`CYL_Max` AS `Cyl_Tol`, results.`CYL_InTol` AS `Cyl_Ver`,
            results.`LDAX` AS `Ax_Nom`, results.`AX_MES` AS `Ax_Mes`, results.`AX_Max` AS `Ax_Tol`, results.`AX_InTol` AS `Ax_Ver`,
            results.`LDPRVM` AS `PD_Nom`, results.`PRVM_MES` AS `PD_Mes`, results.`PRVM_Max` AS `PD_Tol`, results.`PRVM_Min` AS `PD_Min`, results.`PRVM_InTol` AS `PD_Ver`,
            results.`LDPRVA` AS `PA_Nom`, results.`PRVA_MES` AS `PA_Mes`, results.`PRVA_Max` AS `PA_Tol`, results.`PRVA_Min` AS `PA_Min`, results.`PRVA_InTol` AS `PA_Ver`,
            results.`LDADD` AS `Add_Nom`, results.`ADD_MES` AS `Add_Mes`, results.`ADD_Max` AS `Add_Tol`, results.`ADD_InTol` AS `Add_Ver`,
            results.`LDCTHK` AS `Thk_Nom`, results.`CTHICK_MES` AS `Thk_Mes`, results.`CTHICK_Max` AS `Thk_Tol`, results.`CTHICK_Min` AS `Thk_Min`, results.`CTHICK_InTol` AS `Thk_Ver`,
            results.`LDPRVH` AS `PH_Nom`, results.`PRVH_MES` AS `PH_Mes`, results.`PRVH_Max` AS `PH_Tol`, results.`PRVH_Min` AS `PH_Min`, results.`PRVH_InTol` AS `PH_Ver`,
            results.`LDPRVV` AS `PV_Nom`, results.`PRVV_Mes` AS `PV_Mes`, results.`PRVV_Max` AS `PV_Tol`, results.`PRVV_Max` AS `PV_Min`, results.`PRVV_InTol` AS `PV_Ver`,
            results.`GripperNum` AS `Gripper`,
            results.`ErrorId` AS `Status`, results.`ErrorMsg` AS `Error`, results.`ErrorGroupId` AS `Category`
        FROM results
        ORDER BY `CreatedDate` DESC
        LIMIT 75000;
    """

OUTPUT_FILE = "Kickouts_Staged.csv"
lock = threading.Lock()


# populate ip, sn, db_type, user, pwd from sn_list and query from get_xx_query defined aboved
# assign to engine >> sn, db_type
# assign to query >> user, pw, ip

# switched from [for row in sn_list] loop statement to helper function [def fetch_data]
# def handles one row at a time instead of iterating over
# def fetch_data is passed to ThreadPoolExector for parallel execution
# think of def fetch_data as a template to process each row in sn_list and create a sql connection engine.
# def fetch_data also then connects to the sql db and feeds it into the df


def fetch_data(row):
    ip = row["IP"]
    sn = row["SN"]
    db_type = row["SQL"]
    user = row["Username"]
    pwd = row["Password"]
    lab = row["Lab"]
    mapper = row["Mapper"]
    start_time = time.time()

# create_engine is a sqlalchemy function that we imported in the dependencies
    try:
        if db_type.lower() == "postgresql":
            engine = create_engine(f'postgresql://{user}:{pwd}@{ip}:5432/ar_database')
            query = get_postgres_query(sn)
        elif db_type.lower() == "mysql":
            engine = create_engine(f'mysql+mysqlconnector://{user}:{pwd}@{ip}:3306/ar_database')
            query = get_mysql_query(sn)
        else:
            print(f"Unsupported DB type for SN {sn}")
            return None

# pd.read_sql_query is a built-in function of pandas that returns a dataframe 'df'
# input is usually (sql, connection, index_col,...)
# results.append(df) is altered to return df, because we switched from for loop to function. the appending is handled in ThreadPoolExecutor

        df = pd.read_sql_query(query, engine)
        df["IP"] = ip
        df["SN"] = sn
        df["Lab"] = row["Lab"]
        df["Mapper"] = row["Mapper"]

        elapsed = time.time() - start_time
        print(f"✅ {lab} - {sn} returned {len(df)} rows in {int(elapsed)}s")

        if not df.empty:
            with lock:  # Ensure only one thread writes at a time
                df.to_csv(OUTPUT_FILE, mode="a", header=not os.path.exists(OUTPUT_FILE), index=False)

        return None  # Nothing to append anymore

    except Exception as e:
        print(f"⚠️  Failed to fetch from {lab} - {sn} @ {ip}: {e}")
    
if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

# parallel processing using TPE
# [for row in sn_list] and [results.append(df)] is embedded here
# submit() tells TPE executor to run fetch_data() in a separate thread
# submit() a placeholder object for the result which might not be ready yet

# for statement in submit() is a dictionary comprehension that builds:
# future_to_row = {
#     <Future at 0x...>: row1,
#     <Future at 0x...>: row2,
#     ...
# }

# as_completed(future_to_row) generates futures one at a time as they complete, instead of waiting for all threads to finish. 


with ThreadPoolExecutor(max_workers=9) as executor:
    future_to_row = {executor.submit(fetch_data, row): row for row in sn_list}
    for future in as_completed(future_to_row):
        future.result()  # Already writing to CSV in fetch_data()

print("✅ Data succesfully streamed to Kickouts_Staged.csv")
