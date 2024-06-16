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

def create_view_durations(cursor):
    cursor.execute("DROP VIEW IF EXISTS issue_status_durations")

    cursor.execute("""
        CREATE VIEW issue_status_durations AS
        WITH initial_status AS (
            SELECT
                issue_key,
                assignee_key AS author_key,
                Null AS from_status,
                Null AS to_status,
                created_at
            FROM
                issues
        ),
        
        status_changes AS (
            SELECT
                issue_key,
                author_key,
                from_status,
                to_status,
                created_at
            FROM
                changelogs
        ),
        
        combined_statuses AS (
            SELECT
                issue_key,
                author_key,
                from_status,
                to_status,
                created_at
            FROM
                initial_status
            UNION ALL
            SELECT
                issue_key,
                author_key,
                from_status,
                to_status,
                created_at
            FROM
                status_changes
        ),
        
        status_durations AS (
            SELECT
                issue_key,
                author_key,
                from_status, --AS status,
                to_status,
                LAG(created_at) OVER (PARTITION BY issue_key ORDER BY created_at) AS start_date,
                created_at AS end_date
            FROM
                combined_statuses
        )
        
        SELECT
            issue_key,
            author_key,
            from_status,
            to_status,
            start_date,
            end_date,
            ROUND((julianday(end_date) - julianday(start_date)) * 86400,1) AS duration_seconds,
            
            ROUND((julianday(end_date) - julianday(start_date)) * 24, 2) AS duration_hours
            
        FROM
            status_durations
        WHERE
            start_date IS NOT NULL
        --    status = "In Progress"
        ORDER BY
            issue_key,
        --  author_key,
            start_date
    """)

def create_issue_working_hours(cursor):
    cursor.execute("DROP TABLE IF EXISTS issue_working_hours")

    cursor.execute("""
        CREATE TABLE issue_working_hours AS
        SELECT
            issue_key,
            author_key,
            from_status,
            to_status,
            start_date,
            end_date,
            duration_seconds,
            duration_hours,
        
            0 AS working_seconds,
            0 AS working_hours
        FROM
            issue_status_durations
    """)

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

    return round(working_seconds)


def fill_working_hours(cursor):
    # cursor.execute("SELECT * FROM issue_working_hours")

    # Отримати дані з тимчасової таблиці
    cursor.execute(
        "SELECT issue_key, author_key, from_status, to_status, start_date, end_date FROM issue_working_hours")
    rows = cursor.fetchall()

    # Обчислити робочий час для кожного рядка
    for row in rows:
        issue_key, author_key, status, to_status, start_date_str, end_date_str = row
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S")
        working_seconds = calculate_working_hours(start_date, end_date)
        working_hours = round(working_seconds / 3600, 2)
        # print(issue_key, author_key, working_seconds, working_hours)

        # Оновити відповідні записи в тимчасовій таблиці
        cursor.execute("""
              UPDATE issue_working_hours
              SET working_seconds = ?, working_hours = ?
              WHERE issue_key = ? AND author_key = ? AND start_date = ? AND end_date = ?
          """, (working_seconds, working_hours, issue_key, author_key,
                start_date_str, end_date_str))



# Основний блок для виконання всіх дій
if __name__ == "__main__":
    db_name = 'public.sqlite3'

    conn = connect_to_db(db_name)
    if conn:
        cursor = conn.cursor()
        create_view_durations(cursor)
        create_issue_working_hours(cursor)
        fill_working_hours(cursor)

        conn.commit()
        conn.close()
