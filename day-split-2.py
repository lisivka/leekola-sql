import sqlite3
from collections import defaultdict

# Підключення до бази даних
conn = sqlite3.connect('public.sqlite3')
cursor = conn.cursor()

# Вибірка даних
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
cursor.execute(query)
rows = cursor.fetchall()


# Функція для розбиття інтервалів на перекриваючі і неперекриваючі сегменти
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


# Розбиття інтервалів
splits = split_intervals(rows)

# Створення нової таблиці
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

# Вставка даних в нову таблицю
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

# Збереження змін
conn.commit()

# Закриття з'єднання
conn.close()
