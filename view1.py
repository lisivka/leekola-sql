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


# Функція для створення представлення issue_status_durations
def create_issue_status_durations_view(conn):
    try:
        conn.execute('''
        CREATE VIEW IF NOT EXISTS issue_status_durations2 AS
        SELECT
            c1.issue_key,
            c1.author_key,
            c1.to_status AS status,
            c1.created_at AS start_date,
            COALESCE(c2.created_at, '9999-12-31 23:59:59') AS end_date
        FROM
            changelogs c1
        LEFT JOIN
            changelogs c2
        ON
            c1.issue_key = c2.issue_key
            AND c1.created_at < c2.created_at
            AND c2.created_at = (
                SELECT MIN(c3.created_at)
                FROM changelogs c3
                WHERE c3.issue_key = c1.issue_key
                AND c3.created_at > c1.created_at
            )
        ORDER BY
            c1.issue_key, c1.created_at;
        ''')
        conn.commit()
        print("View issue_status_durations created successfully.")
    except sqlite3.Error as e:
        print(f"Error creating view: {e}")


# Основний блок для виконання всіх дій
if __name__ == "__main__":
    db_name = 'public.sqlite3'

    conn = connect_to_db(db_name)
    if conn:
        create_issue_status_durations_view(conn)
        conn.close()
