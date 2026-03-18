#!/usr/bin/env python3
"""
Initialize dummy SQLite database from SQL script
"""

import sqlite3
import os

def init_database():
    """Create and populate the customer database"""
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, 'customers.db')
    sql_script_path = os.path.join(script_dir, 'create_database.sql')
    
    # Remove existing database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed existing database: {db_path}")
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Read and execute SQL script
    with open(sql_script_path, 'r') as f:
        sql_script = f.read()
    
    # Execute each statement
    for statement in sql_script.split(';'):
        statement = statement.strip()
        if statement:
            print(f"Executing: {statement[:60]}...")
            cursor.execute(statement)
    
    conn.commit()
    
    # Verify the data was inserted
    cursor.execute("SELECT COUNT(*) FROM Customer")
    count = cursor.fetchone()[0]
    print(f"\n✓ Database initialized successfully!")
    print(f"✓ Total customers inserted: {count}")
    
    # Show sample data
    print("\nSample data from Customer table:")
    cursor.execute("SELECT * FROM Customer LIMIT 5")
    for row in cursor.fetchall():
        print(f"  {row}")
    
    conn.close()


if __name__ == '__main__':
    init_database()
