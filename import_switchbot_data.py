import pandas as pd
import os
import mysql.connector
from mysql.connector import errorcode

try:
    from config import dbConfig, config_params
except ImportError:
    print("[ERROR]: The file 'config.py' was not found.")
    print("[HINT]: Make sure to rename 'config_sample.py' to 'config.py'.")
    exit(1)

def get_db_connection():
    try:
        conn = mysql.connector.connect(**dbConfig)
        return conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("[ERROR]: Incorrect credentials.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("[ERROR]: Database '{0}' does not exist.".format(dbConfig.get("database")))
        elif err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("[ERROR]: The specified table '{}' does not exist.".format(config_params.get("table_name")))
        else:
            print("[ERROR]: {0}".format(err))
            exit(1)

def create_table_if_not_exists(cursor, table_name):
    # Check if the table exists
    cursor.execute("SHOW TABLES LIKE '{0}'".format(table_name))
    result = cursor.fetchone()
    if result:
        print("[INFO]: Table '{0}' already exists.".format(table_name))
    else:
        create_table_query = """
        CREATE TABLE {0} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Timestamp DATETIME,
            Temperature_Celsius DECIMAL(4,1),
            Relative_Humidity DECIMAL(4,1),
            Absolute_Humidity DECIMAL(4,1),
            DPT_Celsius DECIMAL(4,1),
            VPD DECIMAL(4,2)
        );
        """.format(table_name)
        try:
            cursor.execute(create_table_query)
            print("[INFO]: Table '{0}' is ready.".format(table_name))
        except mysql.connector.Error as err:
            print("[ERROR]: Unable to create table '{0}'. Error: {1}".format(table_name, err))
            exit(1)

def get_csv_file():
    # List all CSV-Files in the current directory
    csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
    
    if not csv_files:
        print("[ERROR]: No CSV-Files found in the current directory. Abort run.")
        exit(1)
    
    if len(csv_files) == 1:
        print("Found one CSV-File: {0}".format(csv_files[0]))
        print("Using this file now.")
        return csv_files[0]
    
    print("Multiple CSV-Files found:")
    for i, file in enumerate(csv_files, 1):
        print(f"{i}. {file}")
    
    while True:
        try:
            choice = int(input("Please choose the number of the correct CSV-File: "))
            if 1 <= choice <= len(csv_files):
                return csv_files[choice - 1]
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please insert a number.")

def import_csv_to_db():
    # CSV-Datei auswählen
    csv_file = get_csv_file()

    # CSV-Datei laden
    try:
        df = pd.read_csv(csv_file)
        if df.empty:
            print("[ERROR]: The chosen CSV-File '{0}' is empty.".format(csv_file))
            exit(1)
    except pd.errors.EmptyDataError:
        print("[ERROR]: The chosen CSV-File '{0}' contains no data.".format(csv_file))
        exit(1)


    # TODO: Dynmic mapping?
    first_date = df['Timestamp'].min()
    last_date = df['Timestamp'].max()
    total_rows = len(df)

    if config_params.get("show_stats"):
        print("Some stats:")
        print("Total number of rows in the CSV-File: {0}".format(total_rows))
        print("First date in the CSV-File: {0}".format(first_date))
        print("Last date in the CSV-File: {0}".format(last_date))

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        table_name = config_params.get("table_name")
        create_table_if_not_exists(cursor, table_name)

        # TODO: Dynmic mapping 
        insert_stmt = (
            "INSERT INTO {0} (Timestamp, Temperature_Celsius, Relative_Humidity, Absolute_Humidity, DPT_Celsius, VPD) "
            "VALUES (%s, %s, %s, %s, %s, %s)".format(config_params.get("table_name"))
        )

        # TODO: Check if column + row exists
        check_stmt = (
            "SELECT COUNT(*) FROM {0} WHERE Timestamp = %s".format(config_params.get("table_name"))
        )

        # Datenzeilen hinzufügen
        imported_rows = 0
        for index, row in df.iterrows():
            cursor.execute(check_stmt, (row['Timestamp'],))
            (count,) = cursor.fetchone()
            if count == 0:
                data = (
                    row['Timestamp'], 
                    row['Temperature_Celsius(°C)'], 
                    row['Relative_Humidity(%)'], 
                    row['Absolute_Humidity(g/m³)'], 
                    row['DPT_Celsius(°C)'], 
                    row['VPD(kPa)']
                )
                cursor.execute(insert_stmt, data)
                imported_rows += 1
                if config_params.get("show_progress") and not ((index + 1) % 100 == 0):
                    progress = (index + 1) / total_rows * 100
                    print("Progress: {:.2f}% done".format(progress))

        # all done - show overall statistic
        print("[INFO]: {0}/{1} Rows processed. {2} new rows imported.".format((index + 1), total_rows, imported_rows))

        # Änderungen übernehmen und Verbindung schließen
        conn.commit()
        cursor.close()
        conn.close()
        print("[INFO]: CSV-File processed with success. In total {0} new rows have been imported.".format(imported_rows))

    except mysql.connector.Error as err:
        print("[ERROR]: {0}".format(err))

if __name__ == "__main__":
    try:
        import_csv_to_db()
    except Exception as ex:
        print("[ERROR]: An unexpected error occurred: {0}".format(ex))

