import sqlite3
from datetime import datetime, timedelta

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
        CREATE VIEW IF NOT EXISTS issue_status_durations AS
        SELECT
            c1.issue_key,
            c1.author_key,
            c1.to_status AS status,
            c1.created_at AS start_date,
            CASE
                WHEN c1.to_status = 'Closed' AND c2.created_at IS NULL THEN c1.created_at
                ELSE COALESCE(c2.created_at, datetime('now'))
            END AS end_date
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

# Функція для створення представлення issue_status_durations_with_seconds
def create_issue_status_durations_with_seconds_view(conn):
    try:
        conn.execute('''
        CREATE VIEW IF NOT EXISTS issue_status_durations_with_seconds AS
        SELECT
            issue_key,
            author_key,
            status,
            start_date,
            end_date,
            ROUND((julianday(end_date) - julianday(start_date)) * 86400) AS duration_in_seconds,
            ROUND((julianday(end_date) - julianday(start_date)) * 86400 / 3600.0, 1) AS duration_in_hours
        FROM
            issue_status_durations;
        ''')
        conn.commit()
        print("View issue_status_durations_with_seconds created successfully.")
    except sqlite3.Error as e:
        print(f"Error creating view: {e}")

# Функція для обчислення робочого часу в статусі "In Progress"
def calculate_working_hours(start, end):
    start_work = start.replace(hour=10, minute=0, second=0, microsecond=0)
    end_work = start.replace(hour=20, minute=0, second=0, microsecond=0)
    working_seconds = 0

    while start < end:
        if start.weekday() < 5:  # Понеділок-п'ятниця
            if start < start_work:
                start = start_work
            if start > end_work:
                start += timedelta(days=1)
                continue
            if end < start_work:
                break
            if end > end_work:
                working_seconds += (end_work - start).total_seconds()
                start += timedelta(days=1)
                continue
            working_seconds += (end - start).total_seconds()
            break
        start += timedelta(days=1)
        start = start.replace(hour=10, minute=0, second=0, microsecond=0)

    return working_seconds

# Функція для створення тимчасової таблиці для робочого часу
def create_in_progress_working_hours_table(conn):
    try:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS in_progress_working_hours_temp (
            issue_key TEXT,
            author_key TEXT,
            status TEXT,
            start_date TEXT,
            end_date TEXT,
            working_seconds INTEGER
        );
        ''')

        cursor = conn.execute('SELECT issue_key, author_key, status, start_date, end_date FROM issue_status_durations WHERE status = "In progress"')
        rows = cursor.fetchall()

        for row in rows:
            issue_key, author_key, status, start_date, end_date = row
            start_date_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
            end_date_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
            working_seconds = calculate_working_hours(start_date_dt, end_date_dt)

            conn.execute('''
            INSERT INTO in_progress_working_hours_temp (issue_key, author_key, status, start_date, end_date, working_seconds)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (issue_key, author_key, status, start_date, end_date, working_seconds))

        conn.commit()
        print("Temporary table in_progress_working_hours_temp created and populated successfully.")
    except sqlite3.Error as e:
        print(f"Error creating or populating table: {e}")

# Основний блок для виконання всіх дій
if __name__ == "__main__":
    db_name = 'public.sqlite3'

    conn = connect_to_db(db_name)
    if conn:
        create_issue_status_durations_view(conn)
        create_issue_status_durations_with_seconds_view(conn)
        create_in_progress_working_hours_table(conn)
        conn.close()
