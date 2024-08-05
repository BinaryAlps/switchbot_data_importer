import pandas as pd
import mysql.connector
from mysql.connector import errorcode

# Lesen der CSV-Datei
print("Opening file...")
df = pd.read_csv('data.csv')
print(" -- Done")

# Verbindung zur MySQL-Datenbank herstellen
print("Trying to connect...")
config = {
    'user': '',
    'password': '',
    'host': '',
    'database': ''
}

# Erste und letzte Datum in der CSV-Datei
first_date = df['Timestamp'].min()
last_date = df['Timestamp'].max()
total_rows = len(df)

print(f"Gesamtanzahl der Zeilen in der CSV-Datei: {total_rows}")
print(f"Erstes Datum in der CSV-Datei: {first_date}")
print(f"Letztes Datum in der CSV-Datei: {last_date}")

try:
    # Verbindung zur MySQL-Datenbank herstellen
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # SQL-Insert-Befehl
    insert_stmt = (
        "INSERT INTO switchbot_outdoor (Timestamp, Temperature_Celsius, Relative_Humidity, Absolute_Humidity, DPT_Celsius, VPD) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )

    # Überprüfung, ob der `Timestamp` bereits existiert
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
        
        # Fortschritt anzeigen
        if (index + 1) % 100 == 0 or (index + 1) == total_rows:
            print(f"Fortschritt: {index + 1}/{total_rows} Zeilen verarbeitet, {imported_rows} neue Zeilen importiert.")

    # Änderungen übernehmen und Verbindung schließen
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Daten erfolgreich in die Datenbank importiert. Insgesamt {imported_rows} neue Zeilen importiert.")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Fehler: Zugangsdaten sind inkorrekt.")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Fehler: Die Datenbank existiert nicht.")
    else:
        print(f"Fehler: {err}")

print("DONE")
