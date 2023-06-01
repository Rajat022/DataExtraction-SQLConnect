import pyodbc
import pandas as pd
import datetime
import csv
import numpy as np

def connect_to_database():
    # Check available ODBC drivers
    pyodbc.drivers()

    # Connecting SQL Server database
    conn = pyodbc.connect(
        Trusted_Connection="Yes",
        Driver='{ODBC Driver 17 for SQL Server}',
        Server='STUDENT-LAPTOP\SQLEXPRESS',
        Database='Test'
    )
    return conn


def create_table(cursor):
    # Create the table in the database
    cursor.execute('''
        CREATE TABLE Country8(
            [Geoname ID] INT,
            [Name] NVARCHAR(255),
            [ASCII Name] NVARCHAR(255),
            [Alternate Names] NVARCHAR(255),
            [Feature Class] NVARCHAR(50),
            [Feature Code] NVARCHAR(50),
            [Country Code] NVARCHAR(10),
            [Country name EN] NVARCHAR(255),
            [Country Code 2] NVARCHAR(255),  
            [Admin1 Code] NVARCHAR(20),
            [Admin2 Code] NVARCHAR(80),
            [Admin3 Code] NVARCHAR(20),
            [Admin4 Code] NVARCHAR(20),
            [Population] FLOAT,  
            [Elevation] INTEGER,
            [DIgital Elevation Model] INTEGER,
            [Timezone] NVARCHAR(50),
            [Modification date] DATE,
            [LABEL EN] NVARCHAR(255),
            [Coordinates] NVARCHAR(255)
        )
    ''')


def insert_data(cursor, df):
    # Insert data into the table from the DataFrame
    for row in df.itertuples():
        # Truncate the Alternate Names
        if isinstance(row[4], str):
            alternate_names = row[4][:255]
        else:
            alternate_names = str(row[4])

        # Convert  date string to a datetime object
        modification_date_str = row[18]
        modification_date = datetime.datetime.strptime(modification_date_str, '%Y-%m-%d')

        cursor.execute('''
            INSERT INTO Country8([Geoname ID], [Name], [ASCII Name], [Alternate Names], [Feature Class], [Feature Code], [Country Code], [Country name EN], [Country Code 2], [Admin1 Code], [Admin2 Code], [Admin3 Code], [Admin4 Code], [Population], [Elevation], [DIgital Elevation Model], [Timezone], [Modification date], [LABEL EN], [Coordinates])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (row[1], row[2], row[3], alternate_names, row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], modification_date, row[19], row[20]))


def execute_query(cursor, query):
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows


def write_result_to_tsv(rows, filename):
    # Write the result to the TSV file
    with open(filename, 'w', newline='', encoding='utf-8') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')
        writer.writerows(rows)


def update_data():
    try:
        # Connect to the database
        conn = connect_to_database()
        cursor = conn.cursor()

        # Read the CSV file directly from the URL into a DataFrame
        url = 'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/geonames-all-cities-with-a-population-1000/exports/csv?lang=en&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B'
        df = pd.read_csv(url, delimiter=';')

        #Clenaing and add values 
        df.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        df.fillna(0, inplace=True)
        create_table(cursor)
        insert_data(cursor, df)

        # Perform the SQL query 
        query = '''
        SELECT [Country Code], [Country name EN], Name
        FROM Country8
        WHERE [Country Code] NOT IN (
            SELECT DISTINCT [Country Code]
            FROM Country8
            WHERE Population > 10000000
        )
        ORDER BY CASE WHEN [Country name EN] = '0' THEN 1 ELSE 0 END, [Country name EN]
        '''
        rows = execute_query(cursor, query)
        filename = 'FinalResult.tsv'
        write_result_to_tsv(rows, filename)

    except Exception as e:
        print(f"Error occurred: {str(e)}")

    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    update_data()
