# Data-Engineering-ETL-basic
Basic ETL process to showcase the ETL process of pulling data from an API to Loading it to a local SQL server

    Streams data from an API, formats it, and inserts it into a SQL Server database.
    The function performs the following steps:
    1. Fetches data from an API.
    2. Formats the fetched data.
    3. Creates a table in SQL Server if it does not exist.
    4. Continuously fetches, formats, and inserts data into the SQL Server table every second for 3 minutes.
        str: A message indicating that data has been streamed.

strem_data.py
   |___ sql_server_stream.py

