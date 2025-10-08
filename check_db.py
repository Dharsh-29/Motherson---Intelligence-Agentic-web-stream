"""
Check database contents
"""
import sqlite3

db_path = "motherson_graph.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("=" * 70)
print("DATABASE STATISTICS")
print("=" * 70)

# Count tables
tables = ['companies', 'divisions', 'facilities', 'events', 'sources', 'evidence', 'jobs']

for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"{table:15} {count:5} rows")

print("\n" + "=" * 70)
print("SAMPLE FACILITIES")
print("=" * 70)

cursor.execute("""
    SELECT f.name, d.name, f.city, f.state
    FROM facilities f
    JOIN divisions d ON f.division_id = d.id
    LIMIT 10
""")

for row in cursor.fetchall():
    facility, division, city, state = row
    print(f"{facility:30} | {division:20} | {city or 'N/A':15} | {state or 'N/A'}")

print("\n" + "=" * 70)
print("SAMPLE EVENTS")
print("=" * 70)

cursor.execute("""
    SELECT f.name, e.event_type, e.status, e.event_date
    FROM events e
    JOIN facilities f ON e.facility_id = f.id
    LIMIT 10
""")

for row in cursor.fetchall():
    facility, event_type, status, event_date = row
    print(f"{facility:30} | {event_type:15} | {status or 'N/A':20} | {event_date or 'N/A'}")

print("\n" + "=" * 70)
print("JOBS")
print("=" * 70)

cursor.execute("SELECT title, location, is_factory_role FROM jobs LIMIT 10")
jobs = cursor.fetchall()

if jobs:
    for row in jobs:
        title, location, is_factory = row
        role_type = "Factory" if is_factory else "Non-factory"
        print(f"{title:40} | {location or 'N/A':20} | {role_type}")
else:
    print("No jobs found")

conn.close()