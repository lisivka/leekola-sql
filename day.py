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


# Функція для обчислення робочого часу в статусі "In progress"
def calculate_working_hours(start, end):
    start_work = datetime(start.year, start.month, start.day, 10, 0, 0)
    end_work = datetime(start.year, start.month, start.day, 20, 0, 0)
    working_seconds = 0

    while start < end:
        if start.weekday() < 5:  # Понеділок-п'ятниця
            if start < start_work:
                start = start_work
            if start > end_work:
                start = start.replace(day=start.day + 1, hour=10, minute=0,
                                      second=0)
                continue
            if end < start_work:
                break
            if end > end_work:
                working_seconds += (end_work - start).total_seconds()
                start = start.replace(day=start.day + 1, hour=10, minute=0,
                                      second=0)
                continue
            working_seconds += (end - start).total_seconds()
            break
        start += timedelta(days=1, hours=10 - start.hour, minutes=-start.minute,
                           seconds=-start.second)

    return working_seconds


# Функція для створення представлення для робочого часу
def create_in_progress_working_hours_view(conn):
    try:
        conn.execute('''
        CREATE VIEW IF NOT EXISTS in_progress_working_hours AS
        SELECT
            issue_key,
            author_key,
            status,
            start_date,
            end_date,
            0 AS working_seconds -- Temporary column to be updated
        FROM
            issue_status_durations
        WHERE
            status = 'In progress';
        ''')

        cursor = conn.execute(
            'SELECT issue_key, author_key, start_date, end_date FROM in_progress_working_hours')
        rows = cursor.fetchall()

        for row in rows:
            issue_key, author_key, start_date, end_date = row
            start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
            end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
            working_seconds = calculate_working_hours(start_date, end_date)

            conn.execute('''
            UPDATE in_progress_working_hours
            SET working_seconds = ?
            WHERE issue_key = ? AND author_key = ? AND start_date = ? AND end_date = ?
            ''', (working_seconds, issue_key, author_key, start_date, end_date))

        conn.commit()
        print(
            "View in_progress_working_hours created and populated successfully.")
    except sqlite3.Error as e:
        print(f"Error creating or populating view: {e}")


# Основний блок для виконання всіх дій
if __name__ == "__main__":
    db_name = 'public.sqlite3'

    conn = connect_to_db(db_name)
    if conn:
        create_issue_status_durations_view(conn)
        create_issue_status_durations_with_seconds_view(conn)
        create_in_progress_working_hours_view(conn)
        conn.close()
