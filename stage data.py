import pandas as pd
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import os
import sys
from dotenv import load_dotenv, set_key

# concurrent.futures is a built-in Python library
# TPE: allows parallel thread processing
# as_completed: iterate over results as soon as each task is finished regardless of order

# ─────────────────────────────────────────────
# CONFIGURATION — Add or comment out connections here
# IP, Lab, SN, Mapper, and DB type only — no credentials
# ─────────────────────────────────────────────
# List of all SQL Connections 
sn_list = [
    {"Lab": "xxx", "IP": "x.x.x.x", "SN": "123", "Mapper": "v2", "SQL": "PostgreSQL"},
    {"Lab": "xxx", "IP": "x.x.x.x", "SN": "134", "Mapper": "v3", "SQL": "MySQL"},
    # Add additional connections here. Comment out any row to skip on next run.
]

ENV_FILE = ".env"
OUTPUT_FILE = "Kickouts_Staged.csv"
lock = threading.Lock()
MAX_RETRIES = 3

# ─────────────────────────────────────────────
# CROSS-PLATFORM PASSWORD INPUT
# Displays asterisks as the user types instead of showing nothing.
# Uses msvcrt on Windows, termios on Linux/Mac — both are built-in.
# ─────────────────────────────────────────────

def password_input(prompt="   Password: "):
    print(prompt, end="", flush=True)
    if os.name == "nt":                         # Windows
        import msvcrt
        pwd = ""
        while True:
            ch = msvcrt.getwch()
            if ch in ("\r", "\n"):              # Enter key
                print()
                break
            elif ch == "\x08":                  # Backspace
                if pwd:
                    pwd = pwd[:-1]
                    print("\b \b", end="", flush=True)
            elif ch == "\x03":                  # Ctrl+C
                raise KeyboardInterrupt
            else:
                pwd += ch
                print("*", end="", flush=True)
        return pwd
    else:                                       # Linux / Mac
        import termios, tty
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        pwd = ""
        try:
            tty.cbreak(fd)
            while True:
                ch = sys.stdin.read(1)
                if ch in ("\r", "\n"):          # Enter key
                    print()
                    break
                elif ch == "\x7f":              # Backspace
                    if pwd:
                        pwd = pwd[:-1]
                        print("\b \b", end="", flush=True)
                elif ch == "\x03":              # Ctrl+C
                    raise KeyboardInterrupt
                else:
                    pwd += ch
                    print("*", end="", flush=True)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
        return pwd

# ─────────────────────────────────────────────
# CREDENTIAL MANAGEMENT
# Checks .env for saved credentials keyed by SN.
# If not found, prompts the user and verifies with a test connection.
# On success, saves to .env for future runs.
# ─────────────────────────────────────────────

load_dotenv(ENV_FILE)

def get_env_key(sn, field):
    """Build a consistent .env key from SN and field name. e.g. DB_USER_123"""
    return f"DB_{field.upper()}_{sn}"

def test_connection(ip, db_type, user, pwd):
    """Attempt a lightweight test connection. Returns True if successful."""
    try:
        if db_type.lower() == "postgresql":
            engine = create_engine(f'postgresql://{user}:{pwd}@{ip}:5432/ar_database')
        elif db_type.lower() == "mysql":
            engine = create_engine(f'mysql+mysqlconnector://{user}:{pwd}@{ip}:3306/ar_database')
        else:
            return False
        with engine.connect() as conn:
            pass
        return True
    except Exception:
        return False

def resolve_credentials(row):
    """
    For a given connection row, return (username, password).
    Checks .env first. If not found, prompts the user interactively.
    Retries up to MAX_RETRIES times on failed connections.
    Returns (None, None) if all attempts fail or input is skipped.
    """
    sn = row["SN"]
    ip = row["IP"]
    db_type = row["SQL"]
    lab = row["Lab"]

    user_key = get_env_key(sn, "USER")
    pass_key = get_env_key(sn, "PASSWORD")

    saved_user = os.getenv(user_key)
    saved_pass = os.getenv(pass_key)

    # Credentials already saved — use them
    if saved_user and saved_pass:
        print(f"🔑 {lab} - {sn}: Using saved credentials.")
        return saved_user, saved_pass

    # No saved credentials — prompt the user
    print(f"\n🔐 New connection: {lab} - {sn} ({ip}, {db_type})")
    print(f"   No saved credentials found. Please enter credentials.")

    for attempt in range(1, MAX_RETRIES + 1):
        user = input(f"   Username: ").strip()
        pwd = password_input()

        if not user or not pwd:
            print(f"   ⚠️  Skipping {lab} - {sn} (no credentials entered).")
            return None, None

        print(f"   Verifying connection... ", end="", flush=True)
        if test_connection(ip, db_type, user, pwd):
            print("✅ Success.")
            # Save to .env for future runs
            set_key(ENV_FILE, user_key, user)
            set_key(ENV_FILE, pass_key, pwd)
            print(f"   Credentials saved to {ENV_FILE}.")
            return user, pwd
        else:
            print(f"❌ Failed.")
            if attempt < MAX_RETRIES:
                print(f"   Attempt {attempt}/{MAX_RETRIES} failed. Try again.")
            else:
                print(f"   ⚠️  All {MAX_RETRIES} attempts failed. Skipping {lab} - {sn}.")
                return None, None

# ─────────────────────────────────────────────
# RESOLVE ALL CREDENTIALS BEFORE THREADING
# Credential prompts must happen in the main thread (user-facing).
# All prompts are handled upfront so threads can run silently.
# ─────────────────────────────────────────────

print("\n── Verifying credentials ──────────────────────")
resolved = []
for row in sn_list:
    user, pwd = resolve_credentials(row)
    if user and pwd:
        resolved.append({**row, "Username": user, "Password": pwd})
    else:
        print(f"   Skipping {row['Lab']} - {row['SN']}.")

if not resolved:
    print("\n⚠️  No valid connections. Exiting.")
    exit()

print(f"\n── Starting data pull ({len(resolved)} connections) ──────")

# ─────────────────────────────────────────────
# SQL QUERIES
# ─────────────────────────────────────────────

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

# ─────────────────────────────────────────────
# DATA FETCH
# Handles one connection at a time, passed to ThreadPoolExecutor for parallel execution.
# ─────────────────────────────────────────────

def fetch_data(row):
    ip = row["IP"]
    sn = row["SN"]
    db_type = row["SQL"]
    user = row["Username"]
    pwd = row["Password"]
    lab = row["Lab"]
    start_time = time.time()

    # create_engine is a sqlalchemy function that builds the connection string
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

        # pd.read_sql_query returns a dataframe from the query result
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

        return None

    except Exception as e:
        print(f"⚠️  Failed to fetch from {lab} - {sn} @ {ip}: {e}")

# ─────────────────────────────────────────────
# EXECUTION
# ─────────────────────────────────────────────

if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

# parallel processing using TPE
# submit() tells TPE to run fetch_data() in a separate thread for each connection
# future_to_row is a dictionary comprehension mapping each Future to its row
# as_completed() yields futures one at a time as they finish, without waiting for all threads

with ThreadPoolExecutor(max_workers=9) as executor:
    future_to_row = {executor.submit(fetch_data, row): row for row in resolved}
    for future in as_completed(future_to_row):
        future.result()

print("\n✅ Data successfully streamed to Kickouts_Staged.csv")
