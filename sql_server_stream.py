
from datetime import datetime
import pyodbc
import pandas as pd
import pyodbc


default_args = {
    'owner' : 'kabelo',
    'start_date': datetime(2025,2,4),

}


def sql_connector():
    """
    Function to establish a connection to the SQL Server
    Input; None
    Returns:
        pyodbc connection object: Connection object to the SQL Server
    """
# Establish a connection to the database
    cnxn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost;" # Replace with your server name
        "Database=KABELODB;"
        "UID=KABELO_ZENBOOK\kabel;"  # Replace with your username
        "PWD='';"  # Replace with your password
        "TrustServerCertificate=yes;"
        "Trusted_Connection=yes;" )     

    return cnxn



table_query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'kp01' AND TABLE_NAME = 'users')
    BEGIN
        CREATE TABLE kp01.users (
            [First Name] NVARCHAR(50),
            [Last Name] NVARCHAR(50),
            [Location] NVARCHAR(MAX),
            [Email] NVARCHAR(100),
            [DOB] DATETIME,
            [Registration Date] DATETIME,
            [Registration Age] INT,
            [Cell Number] NVARCHAR(20),
            [ID] NVARCHAR(50)
        )
    END
    """
def create_table(table_query, cnxn = sql_connector()):

    cursor = cnxn.cursor()
    cursor.execute(table_query)
    cnxn.commit()

    return 




# Insert DataFrame into SQL Server
insert_statement = """
INSERT INTO kp01.users ([First Name], [Last Name], [Location], [Email], [DOB], [Registration Date], [Registration Age], [Cell Number], [ID])
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
def insert_data(datas,insert_statement, cnxn = sql_connector()):
    """
    Function to insert data into the SQL Server
    Input: 
        datas: List of data to be inserted
        insert_statement: SQL insert statement
        cnxn: Connection object to the SQL Server
        Returns: None
    """
    df = pd.DataFrame({
    'First Name': datas[0],
    'Last Name': datas[1],
    'Location': datas[2],
    'Email': datas[3],
    'DOB': datas[4],
    'Registration Date': datas[5],
    'Registration Age': datas[6],
    'Cell Number': datas[7],
    'ID': datas[8]
    })

    cursor = cnxn.cursor()
    for index, row in df.iterrows():
        cursor.execute(insert_statement, row['First Name'], row['Last Name'], row['Location'], row['Email'], row['DOB'], row['Registration Date'], row['Registration Age'], row['Cell Number'], row['ID'])

    cnxn.commit()
    cursor.close()
    #cnxn.close()

    return





