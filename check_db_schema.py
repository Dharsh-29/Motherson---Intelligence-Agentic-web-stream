"""
Quick script to check if 'country' column exists in facilities table
Run this after pipeline to verify schema
"""
import sqlite3

db_path = "motherson_graph.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check facilities table schema
cursor.execute("PRAGMA table_info(facilities)")
columns = cursor.fetchall()

print("=" * 70)
print("FACILITIES TABLE SCHEMA")
print("=" * 70)
for col in columns:
    print(f"{col[1]:20} {col[2]:15} {'NOT NULL' if col[3] else 'NULL'}")

# Check if country column exists
has_country = any(col[1] == 'country' for col in columns)

if not has_country:
    print("\n⚠️  WARNING: 'country' column NOT FOUND!")
    print("Adding 'country' column with default 'India'...")
    
    cursor.execute("ALTER TABLE facilities ADD COLUMN country TEXT DEFAULT 'India'")
    cursor.execute("UPDATE facilities SET country = 'India' WHERE country IS NULL")
    conn.commit()
    
    print("✅ Added 'country' column")
else:
    print("\n✅ 'country' column exists")
    
    # Check country distribution
    cursor.execute("SELECT country, COUNT(*) FROM facilities GROUP BY country")
    countries = cursor.fetchall()
    
    print("\nCountry Distribution:")
    for country, count in countries:
        print(f"  {country or 'NULL':20} {count:5} facilities")

conn.close()
print("\n" + "=" * 70)