# Multi-Threaded SQL Data Staging in Python

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=flat&logo=postgresql&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=flat&logo=mysql&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=flat&logo=powerbi&logoColor=black)
![Threading](https://img.shields.io/badge/Concurrency-ThreadPoolExecutor-success?style=flat)
![License](https://img.shields.io/badge/License-MIT-lightgrey?style=flat)

> A multi-threaded Python data-staging tool that replaces Power BI's native SQL connectors with concurrent database pulls.
> Reduces total dashboard refresh time by **48%** across 19 simultaneous database connections.

---

## Background

Power BI's built-in SQL connectors handle multi-source refreshes inefficiently, creating operational inefficiencies at scale.

- **Unoptimized parallel loading:** Power BI's refresh logic conducts query folding and schema inference for each data source on every refresh.
- **Crashes and timeouts:** Too many simultaneous ODBC connections overload the network stack, especially as row counts grow.
- **Configuration overhead:** Managing multiple data sources via Data Source Wizards or M-Code parameterization is error-prone and brittle.

---

## Architecture

This script acts as a staging layer between your SQL databases and Power BI. Instead of letting Power BI manage connections, Python handles all data extraction concurrently and writes a single clean CSV. Power BI connects to one static file.

```
┌─────────────────────────────────────────────┐
│              Config: DB List                │
│   [postgres_1, postgres_2, ..., mysql_3]    │
└────────────────────┬────────────────────────┘
                     │
                     ▼
        ┌────────────────────────┐
        │   ThreadPoolExecutor   │  ← Concurrent pulls
        │  (N workers, N = DBs)  │
        └────┬──────┬──────┬─────┘
             │      │      │
           DB_1    DB_2   DB_N    ← Postgres & MySQL supported
             │      │      │
        └────┴──────┴──────┘
                     │
                     ▼
           ┌─────────────────┐
           │  Merged Dataset │
           └────────┬────────┘
                    │
                    ▼
           ┌─────────────────┐
           │  Single CSV     │  ← Power BI connects here
           └─────────────────┘
```

---

## Benchmark

Tested across **19 databases** (3 MySQL, 16 PostgreSQL) with a **50,000 row limit per database** (~950,000 total rows). Each configuration was run 10 times and averaged.

| Metric | Power BI Native | Python Staging + Power BI | Δ |
|---|---|---|---|
| Data Pull / Refresh | 224 sec | 78 sec (Python) | −146 sec |
| Power BI Refresh | — | 38 sec | — |
| **Total Cycle Time** | **224 sec** | **116 sec** | **−108 sec (−48%)** |

> The Python script runs first, then Power BI refreshes against the exported CSV. The operator initiates both steps, but the combined time is still 48% faster than the native approach — with significantly better stability and zero crashes during testing.

---

## Getting Started

### Prerequisites

```bash
pip install -r requirements.txt
```

### Configuration

Edit `sn_list` in `stage_data.py` to define your database connections. Each entry takes connection metadata only — no credentials:

```python
sn_list = [
    {"Lab": "Lab_A", "IP": "192.168.x.x", "SN": "123", "Mapper": "v2", "SQL": "PostgreSQL"},
    {"Lab": "Lab_B", "IP": "192.168.x.x", "SN": "134", "Mapper": "v3", "SQL": "MySQL"},
    # Add additional connections here — comment out any row to skip on next run
]
```

### Credential Management

Credentials are never stored in the script. On first run, the script will prompt you for a username and password for each new connection, verify the credentials with a live test connection, and save them to a local `.env` file for all future runs.

```
── Verifying credentials ──────────────────────
🔐 New connection: Lab_A - 123 (192.168.x.x, PostgreSQL)
   No saved credentials found. Please enter credentials.
   Username: your_username
   Password: ********
   Verifying connection... ✅ Success.
   Credentials saved to .env.

🔑 Lab_B - 134: Using saved credentials.
```

On all subsequent runs, saved credentials are loaded automatically and no prompt is shown.

### Run

```bash
python stage_data.py
```

Output: `Kickouts_Staged.csv` — point your Power BI report to this file as its single data source.

---

## Customization

The script is modular by design. Common modifications:

- **Export to a SQL table instead of CSV:** swap the `pandas.to_csv()` call for a `DataFrame.to_sql()` call targeting a staging database
- **Add more DB types:** extend the connection handler with an `elif db_type == "mssql"` block using `pyodbc`
- **Schedule execution:** wrap with Windows Task Scheduler or a cron job to automate the staging step before scheduled Power BI refreshes
- **Increase/decrease thread count:** adjust `max_workers` in `ThreadPoolExecutor` based on your network and DB server capacity

---

## License

MIT
