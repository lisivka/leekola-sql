import sqlite3


# Функція для підключення до бази даних SQLite
def connect_to_db(db_name):
    try:
        conn = sqlite3.connect(db_name)
        print(f"Connected to database {db_name}")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def drop_table(cursor, table_name):
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    print(f"Table {table_name} dropped successfully")

# Функція для створення таблиці changelogs,isuues
def create_table(cursor):

    conn.execute("""
        CREATE TABLE changelogs (
        issue_key text,
        author_key text,
        from_status text,
        to_status text,
        created_at timestamp without time zone
        )
        """
                 )

    conn.execute("""
        CREATE TABLE issues (
        issue_key text,
        project_key text,
        issue_type text,
        assignee_key text,
        created_at timestamp without time zone
        )
        """
                 )

    conn.commit()
    print("Tables created successfully")


# Функція для виконання SQL команд з файлу
def execute_sql_from_file(conn, file_path):
    try:
        with open(file_path, 'r') as file:
            sql = file.readline()
            while sql:
                # Видаляємо префікс "public." з SQL запитів
                sql = sql.replace('public.', '')
                try:
                    conn.execute(sql)
                    conn.commit()
                    print(f"Executed: {sql.strip()}")
                except sqlite3.Error as e:
                    print(f"Error executing SQL: {sql.strip()} - {e}")
                sql = file.readline()
    except IOError as e:
        print(f"Error reading file: {e}")


# Функція для видалення всіх записів з таблиць changelogs та issues
def clear_tables(conn):
    try:
        conn.execute("DELETE FROM changelogs")
        conn.execute("DELETE FROM issues")
        conn.commit()
        print(
            "All records from changelogs and issues tables have been deleted.")
    except sqlite3.Error as e:
        print(f"Error clearing tables: {e}")


# Основний блок для виконання всіх дій
if __name__ == "__main__":
    db_name = 'public.sqlite3'
    file_path = 'dump.sql'

    conn = connect_to_db(db_name)
    if conn:
        drop_table(conn, 'changelogs')
        drop_table(conn, 'issues')

        create_table(conn)
        clear_tables(conn)
        execute_sql_from_file(conn, file_path)
        conn.close()
