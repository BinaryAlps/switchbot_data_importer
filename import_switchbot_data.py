import pandas as pd
import os
import mysql.connector
from mysql.connector import errorcode

try:
    from config import config
except ImportError:
    print("[ERROR]: The file 'config.py' was not found.")
    print("[HINT]: Make sure to rename 'config_sample.py' to 'config.py'.")
    exit(1)

def get_csv_file():
    # Alle CSV-Dateien im aktuellen Verzeichnis suchen
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

print("Some stats:")
print("Total number of rows in the CSV-File: {0}".format(total_rows))
print("First date in the CSV-File: {0}".format(first_date))
print("Last date in the CSV-File: {0}".format(last_date))

try:
    # Verbindung zur MySQL-Datenbank herstellen
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # TODO: Dynmic mapping + tablename from config
    insert_stmt = (
        "INSERT INTO switchbot_outdoor (Timestamp, Temperature_Celsius, Relative_Humidity, Absolute_Humidity, DPT_Celsius, VPD) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )

    # TODO: Check if column + row exists
    check_stmt = (
        "SELECT COUNT(*) FROM switchbot_outdoor WHERE Timestamp = %s"
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
        
        # TODO: Check progress 
        if (index + 1) % 100 == 0 or (index + 1) == total_rows:
            print(f"Fortschritt: {index + 1}/{total_rows} Zeilen verarbeitet, {imported_rows} neue Zeilen importiert.")

    # Änderungen übernehmen und Verbindung schließen
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Daten erfolgreich in die Datenbank importiert. Insgesamt {imported_rows} neue Zeilen importiert.")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("[ERROR]: Zugangsdaten sind inkorrekt.")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("[ERROR]: Database '{0}' does not exist.".format(config.get("database")))
    elif err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
        # TODO: dynamic tablename?
        print("[ERROR]: The specified table '{}' does not exist.".format("sone tablename"))
    else:
        print("[ERROR]: {0}".format(err))

print("DONE")
