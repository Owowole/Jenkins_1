import mysql.connector
from mysql.connector import Error

def get_connection(host, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def fetch_data(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()

def migrate_data():
    try:
        # Connexion à la base de données source
        source_conn = get_connection("localhost", "root", "", "jenkis1-database1")
        if source_conn is None:
            return

        source_cursor = source_conn.cursor()

        # Vérifier que les tables T1 et T2 existent
        source_cursor.execute("SHOW TABLES LIKE 'T1'")
        if not source_cursor.fetchone():
            raise Exception("La table T1 n'existe pas dans la base de données source")
        
        source_cursor.execute("SHOW TABLES LIKE 'T2'")
        if not source_cursor.fetchone():
            raise Exception("La table T2 n'existe pas dans la base de données source")

        # Récupérer les données des tables T1 et T2
        rows_T1 = fetch_data(source_cursor, "SELECT C1T1 FROM T1")
        rows_T2 = fetch_data(source_cursor, "SELECT C1T2 FROM T2")

        # Connexion à la base de données de destination
        destination_conn = get_connection("localhost", "root", "", "2jenkis1-database1")
        if destination_conn is None:
            return

        destination_cursor = destination_conn.cursor()

        # Vérifier que la table T3 existe
        destination_cursor.execute("SHOW TABLES LIKE 'T3'")
        if not destination_cursor.fetchone():
            raise Exception("La table T3 n'existe pas dans la base de données de destination")

        # Calculer la longueur maximale
        max_length = max(len(rows_T1), len(rows_T2))

        # Insérer les données dans la table T3
        for i in range(max_length):
            value_C1T2 = rows_T2[i][0] if i < len(rows_T2) else None
            value_C2T1 = rows_T1[i][0] if i < len(rows_T1) else None

            destination_cursor.execute(
                "INSERT INTO T3 (C1T2, C2T1) VALUES (%s, %s)",
                (value_C1T2, value_C2T1)
            )

        destination_conn.commit()
        print("Data migration complete")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if source_cursor:
            source_cursor.close()
        if source_conn and source_conn.is_connected():
            source_conn.close()
        if destination_cursor:
            destination_cursor.close()
        if destination_conn and destination_conn.is_connected():
            destination_conn.close()

if __name__ == "__main__":
    migrate_data()
