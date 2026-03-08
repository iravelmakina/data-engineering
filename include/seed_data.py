"""
Seed script: generates MySQL tables and sample telephony JSON files.
Run once to populate the support_db database and data/telephony/ folder.

Usage:
    pip install faker pymysql python-dotenv
    python include/seed_data.py
"""

import json
import os
import random
from datetime import datetime, timedelta

import pymysql
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

fake = Faker()
Faker.seed(42)
random.seed(42)

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
}

NUM_EMPLOYEES = 50
NUM_CALLS = 120

TEAMS = ["Billing", "Technical", "Sales", "Retention", "General"]
ROLES = ["Agent", "Senior Agent", "Team Lead"]
DIRECTIONS = ["inbound", "outbound"]
STATUSES = ["completed", "missed", "voicemail"]

TELEPHONY_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "telephony")

DESCRIPTIONS = [
    "Customer asked about billing cycle and payment options.",
    "Resolved technical issue with internet connectivity.",
    "Customer requested account cancellation, offered retention deal.",
    "Helped customer update their contact information.",
    "Addressed complaint about service outage in their area.",
    "Assisted with password reset and account security.",
    "Customer inquired about upgrading their current plan.",
    "Processed refund request for duplicate charge.",
    "Provided troubleshooting steps for hardware setup.",
    "Scheduled a technician visit for equipment replacement.",
]


def create_database(cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS support_db")
    cursor.execute("USE support_db")

    cursor.execute("DROP TABLE IF EXISTS calls")
    cursor.execute("DROP TABLE IF EXISTS employees")

    cursor.execute("""
        CREATE TABLE employees (
            employee_id INT PRIMARY KEY AUTO_INCREMENT,
            full_name VARCHAR(255) NOT NULL,
            team VARCHAR(100) NOT NULL,
            role VARCHAR(100) NOT NULL,
            hire_date DATE NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE calls (
            call_id INT PRIMARY KEY AUTO_INCREMENT,
            employee_id INT NOT NULL,
            call_time DATETIME NOT NULL,
            phone VARCHAR(50) NOT NULL,
            direction VARCHAR(10) NOT NULL,
            status VARCHAR(20) NOT NULL,
            FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
        )
    """)
    print("Database and tables created.")


def seed_employees(cursor):
    for _ in range(NUM_EMPLOYEES):
        cursor.execute(
            "INSERT INTO employees (full_name, team, role, hire_date) VALUES (%s, %s, %s, %s)",
            (
                fake.name(),
                random.choice(TEAMS),
                random.choice(ROLES),
                fake.date_between(start_date="-5y", end_date="-6m"),
            ),
        )
    print(f"Inserted {NUM_EMPLOYEES} employees.")


def seed_calls(cursor):
    # Spread calls from 3 hours ago to 3 hours ahead (360 min window).
    # Past calls get picked up on first DAG run.
    # Future calls get picked up by later hourly runs, demonstrating incremental logic.
    base_time = datetime.now() - timedelta(hours=3)
    for i in range(NUM_CALLS):
        call_time = base_time + timedelta(minutes=random.randint(0, 360))
        cursor.execute(
            "INSERT INTO calls (employee_id, call_time, phone, direction, status) VALUES (%s, %s, %s, %s, %s)",
            (
                random.randint(1, NUM_EMPLOYEES),
                call_time,
                fake.phone_number(),
                random.choice(DIRECTIONS),
                random.choice(STATUSES),
            ),
        )
    print(f"Inserted {NUM_CALLS} calls (spread from -3h to +3h).")


def generate_telephony_json():
    os.makedirs(TELEPHONY_DIR, exist_ok=True)
    for call_id in range(1, NUM_CALLS + 1):
        record = {
            "call_id": call_id,
            "duration_sec": random.randint(10, 600),
            "short_description": random.choice(DESCRIPTIONS),
        }
        path = os.path.join(TELEPHONY_DIR, f"call_{call_id}.json")
        with open(path, "w") as f:
            json.dump(record, f, indent=2)

    # Malformed files for error-handling demonstration
    with open(os.path.join(TELEPHONY_DIR, "call_999.json"), "w") as f:
        f.write("{bad json")
    with open(os.path.join(TELEPHONY_DIR, "call_998.json"), "w") as f:
        json.dump({"call_id": 998}, f)  # missing required fields

    print(f"Generated {NUM_CALLS} telephony JSON files + 2 malformed files.")


def main():
    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    try:
        create_database(cursor)
        seed_employees(cursor)
        seed_calls(cursor)
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    generate_telephony_json()
    print("Seed complete.")


if __name__ == "__main__":
    main()
