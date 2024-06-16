import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta


def fetch_data(connection):
    query = """
    SELECT 
        issue_key, 
        author_key, 
        from_status,
        to_status,
        start_date, 
        end_date 
    FROM 
        issue_status_durations 
    WHERE 
        from_status = 'In Progress'
    ORDER BY 
        author_key, 
        start_date;
    """
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def split_intervals(rows):
    events = []
    for row in rows:
        issue_key, author_key, from_status, to_status, start_date, end_date = row
        events.append((start_date, 'start', issue_key, author_key, from_status,
                       to_status, start_date, end_date))
        events.append((end_date, 'end', issue_key, author_key, from_status,
                       to_status, start_date, end_date))

    events.sort()

    active_intervals = defaultdict(list)
    splits = []
    current_time = None

    for event in events:
        event_time, event_type, issue_key, author_key, from_status, to_status, start_date, end_date = event

        if current_time and current_time != event_time:
            for author, intervals in active_intervals.items():
                if intervals:
                    splits.append(
                        (current_time, event_time, intervals.copy(), author))

        if event_type == 'start':
            active_intervals[author_key].append(
                (issue_key, from_status, to_status, start_date, end_date))
        else:
            active_intervals[author_key] = [i for i in
                                            active_intervals[author_key] if
                                            i[0] != issue_key]

        current_time = event_time

    return splits


def create_issue_overlaps_table(connection):
    cursor = connection.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS issue_overlaps (
        issue_key TEXT,
        author_key TEXT,
        from_status TEXT,
        to_status TEXT,
        start_date DATETIME,
        end_date DATETIME,
        start_split DATETIME,
        end_split DATETIME,
        weight INTEGER
    );
    """)


def create_issue_overlaps_working_hours_table(connection):
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS issue_overlaps_working_hours")
    print("Table issue_overlaps_working_hours dropped")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS issue_overlaps_working_hours (
        issue_key TEXT,
        author_key TEXT,
        from_status TEXT,
        to_status TEXT,
        start_date DATETIME,
        end_date DATETIME,
        start_split DATETIME,
        end_split DATETIME,
        weight INTEGER,
        duration_seconds INTEGER,
        duration_hours REAL,
        work_seconds INTEGER,
        work_hours REAL,
        pay REAL
    );
    """)


def insert_overlaps(connection, splits):
    cursor = connection.cursor()
    for split in splits:
        start_split, end_split, interval_data, author_key = split
        weight = len(interval_data)
        for issue_data in interval_data:
            issue_key, from_status, to_status, start_date, end_date = issue_data
            cursor.execute("""
            INSERT INTO issue_overlaps (
                issue_key, 
                author_key, 
                from_status, 
                to_status, 
                start_date, 
                end_date, 
                start_split, 
                end_split, 
                weight
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
            issue_key, author_key, from_status, to_status, start_date, end_date,
            start_split, end_split, weight))
    connection.commit()


def calculate_working_hours(start_split, end_split):
    current = start_split
    total_work_seconds = 0

    while current < end_split:
        if current.weekday() < 5:  # Monday to Friday
            work_start = max(
                current.replace(hour=10, minute=0, second=0, microsecond=0),
                current)
            work_end = min(
                current.replace(hour=20, minute=0, second=0, microsecond=0),
                end_split)
            if work_start < work_end:
                total_work_seconds += (work_end - work_start).total_seconds()
        current += timedelta(days=1)

    return total_work_seconds


def insert_working_hours(connection):
    cursor = connection.cursor()
    cursor.execute(
        "SELECT * FROM issue_overlaps ORDER BY author_key, issue_key, start_split")
    rows = cursor.fetchall()

    for row in rows:
        issue_key, author_key, from_status, to_status, start_date, end_date, start_split, end_split, weight = row
        start_split_dt = datetime.strptime(start_split, '%Y-%m-%d %H:%M:%S')
        end_split_dt = datetime.strptime(end_split, '%Y-%m-%d %H:%M:%S')

        duration_seconds = int((end_split_dt - start_split_dt).total_seconds())
        duration_hours = round(duration_seconds / 3600, 2)

        work_seconds = calculate_working_hours(start_split_dt, end_split_dt)
        work_hours = round(work_seconds / 3600, 2)

        if str(author_key).find('JIRAUSER')==0:
            pay = round(1 * work_hours / weight, 2)
        else:
            pay = round(100 * work_hours / weight, 2)


        cursor.execute("""
        INSERT INTO issue_overlaps_working_hours (
            issue_key, 
            author_key, 
            from_status, 
            to_status, 
            start_date, 
            end_date, 
            start_split, 
            end_split, 
            weight,
            duration_seconds,
            duration_hours,
            work_seconds,
            work_hours,
            pay
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
        issue_key, author_key, from_status, to_status, start_date, end_date,
        start_split, end_split, weight, duration_seconds, duration_hours,
        work_seconds, work_hours, pay))
    connection.commit()


def main():
    connection = sqlite3.connect('public.sqlite3')
    rows = fetch_data(connection)
    splits = split_intervals(rows)
    create_issue_overlaps_table(connection)
    insert_overlaps(connection, splits)
    create_issue_overlaps_working_hours_table(connection)
    insert_working_hours(connection)

    connection.commit()
    connection.close()


if __name__ == "__main__":
    main()
