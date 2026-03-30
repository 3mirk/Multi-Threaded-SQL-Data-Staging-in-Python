# Multi-Threaded-SQL-Data-Staging in Python

Typically, SQL connections are handled via built-in data connectors in Data Visualization tools like Tableau or PowerBI. 
However, connecting simultaneously to a larger-and-larger number of such connections has several downsides. 
These drawbacks can include unoptimized parallel loading, delays due to datatype inferences, and data-handling bottlenecks.

Additionally, setting up a significantly large number of data sources in Data Viz tools can be quite cumbersome or error-prone. 
Using the standard "Data Source Wizards" or M-Code Parametrization both have their own significant limitations.

This Python script serves as a data staging tool that addresses both category of issues by exporting results into a single CSV file. 
The script is also modular, allowing for easy modification and scalability; i.e. results can easily be exported to a SQL Database as opoosed to a CSV File.

This script accomodates the querying of any number of SQL databases via a user-defined list.
The script also dynamically handles varying SQL Database types (currently Postgres and MySQL) allowing the reporting tool to connect directly to a single data source.

For optimization, the script utilizes ThreadPoolExecutor to facilitate simultaneous SQL pulls, which is significantly superior to PowerBI parallel loading. 
This data flow is generally much more secure and handles potential latency or timeout issues much better. 
Too many simultaneous quiries via PowerBI's internal ODBC's can easily crash or timeout, especially as the row count increases. 
Connecting via Python reduces network handshakes that PowerBI may engage for query folding which can quickly add overhead.

I have tested my dashboard in 2 model flows.

**PowerBI:**
* PowerBI directly connected to 19 SQL Databases (3 MySQL, 17 Postgres)
* All Data Refreshes is handled directly inside of PowerBI and Power Query
* 50,000 row limit for each Database * 19 DB's = 950,000 rows
* Average PowerBI Refresh Time (10 Refreshes): 224 seconds

**Python Data-Staging + PowerBI CSV Refresh**
* All 19 SQL connections are handled inside Python Script
* All rows are exported to a single CSV File.
* PowerBI dashboard has only one data source, being this CSV Export.
* Operator has to execute Python Script first, then refresh PowerBI dashboard once the script is completed
* Average Python Script Time (10 Executions): 78 seconds
* Average PowerBI Refresh Time (10 Executions): 38 seconds
* Average Total Refresh Time (10 Runs): 116 seconds
  
**This represents a 108-second (48%) reduction in cycle time.**

Admittedly, direct internal refreshes in PowerBI are slightly more convenient for end users needeing the latest data.
However, the data-staging significantly proves its merit by providing a much more stable and faster refresh cycle-times, avoiding crashes and timeouts.
The gaps between an integrated data pull can be minimized by running this script on certain time intervals.
