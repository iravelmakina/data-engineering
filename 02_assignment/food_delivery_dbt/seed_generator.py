"""
Seed script: generates raw food-delivery CSV files for the dbt project.
Run once to populate the seeds/ folder with referentially consistent fake data.

Usage:
    pip install faker
    python seed_generator.py
"""

import csv
import os
import random
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_CUSTOMERS = 200
NUM_RESTAURANTS = 30
NUM_MENU_ITEMS = 150
NUM_ORDERS = 1500
DAYS_BACK = 90

CUISINES = ["Italian", "japanese", "MEXICAN", "indian", "Chinese", "American", "thai", "French"]
CATEGORIES = ["appetizer", "main", "dessert", "drink", "side"]
PAYMENT_METHODS = ["card", "cash", "wallet"]
ORDER_STATUSES = ["placed", "preparing", "delivered", "cancelled"]
DELIVERY_STATUSES = ["picked_up", "in_transit", "delivered", "failed"]

SEEDS_DIR = os.path.join(os.path.dirname(__file__), "seeds")


def write_csv(filename, header, rows):
    path = os.path.join(SEEDS_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {filename}.")


def seed_customers():
    rows = []
    for i in range(1, NUM_CUSTOMERS + 1):
        first = fake.first_name()
        last = fake.last_name()
        # Intentional dirty data: uppercase emails + trailing spaces in names.
        # Cleaned later by the standardize_text macro in the staging layer.
        email = (
            f"{first}.{last}@{fake.free_email_domain()}".upper()
            if i % 5 == 0
            else f"{first.lower()}.{last.lower()}@{fake.free_email_domain()}"
        )
        rows.append([
            i,
            first + ("  " if i % 7 == 0 else ""),
            last,
            email,
            fake.phone_number(),
            fake.date_between(start_date="-2y", end_date="-90d").isoformat(),
            fake.city(),
        ])
    write_csv(
        "raw_customers.csv",
        ["customer_id", "first_name", "last_name", "email", "phone", "signup_date", "city"],
        rows,
    )


def seed_restaurants():
    rows = []
    for i in range(1, NUM_RESTAURANTS + 1):
        rows.append([
            i,
            fake.company() + " Kitchen",
            random.choice(CUISINES),
            fake.city(),
            round(random.uniform(3.0, 5.0), 2),
            fake.date_between(start_date="-5y", end_date="-1y").isoformat(),
            random.choice([True, True, True, False]),  # ~75% active
        ])
    write_csv(
        "raw_restaurants.csv",
        ["restaurant_id", "name", "cuisine_type", "city", "rating", "opened_date", "is_active"],
        rows,
    )


def seed_menu_items():
    rows = []
    for i in range(1, NUM_MENU_ITEMS + 1):
        rows.append([
            i,
            random.randint(1, NUM_RESTAURANTS),
            fake.word().capitalize() + " " + fake.word().capitalize(),
            random.choice(CATEGORIES),
            round(random.uniform(3.5, 35.0), 2),
            random.choice([True, True, True, False]),
        ])
    write_csv(
        "raw_menu_items.csv",
        ["menu_item_id", "restaurant_id", "item_name", "category", "price_usd", "is_available"],
        rows,
    )


def seed_orders():
    # Spread orders over the last DAYS_BACK days so incremental models
    # have a real date range to filter on. Status weighted realistically
    # (most orders complete; a small share get cancelled).
    rows = []
    now = datetime.now()
    weights = [0.05, 0.05, 0.80, 0.10]  # placed / preparing / delivered / cancelled
    for i in range(1, NUM_ORDERS + 1):
        ts = now - timedelta(
            days=random.randint(0, DAYS_BACK),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        rows.append([
            i,
            random.randint(1, NUM_CUSTOMERS),
            random.randint(1, NUM_RESTAURANTS),
            ts.isoformat(timespec="seconds"),
            random.choices(ORDER_STATUSES, weights=weights, k=1)[0],
            round(random.uniform(8.0, 120.0), 2),
            random.choice(PAYMENT_METHODS),
        ])
    write_csv(
        "raw_orders.csv",
        ["order_id", "customer_id", "restaurant_id", "order_timestamp", "status", "total_amount_usd", "payment_method"],
        rows,
    )
    return rows


def seed_order_items(orders):
    # Each order gets 1-5 line items; FK integrity guaranteed by construction.
    rows = []
    line_id = 1
    for order in orders:
        order_id = order[0]
        for _ in range(random.randint(1, 5)):
            rows.append([
                line_id,
                order_id,
                random.randint(1, NUM_MENU_ITEMS),
                random.randint(1, 4),
                round(random.uniform(3.5, 35.0), 2),
            ])
            line_id += 1
    write_csv(
        "raw_order_items.csv",
        ["order_item_id", "order_id", "menu_item_id", "quantity", "unit_price_usd"],
        rows,
    )


def seed_deliveries(orders):
    # One delivery per non-cancelled order. Pickup/delivery timestamps
    # follow the order timestamp, simulating realistic fulfillment lag.
    rows = []
    delivery_id = 1
    for order in orders:
        order_id, _, _, ts_iso, status, *_ = order
        if status == "cancelled":
            continue
        order_ts = datetime.fromisoformat(ts_iso)
        pickup = order_ts + timedelta(minutes=random.randint(10, 40))
        delivered = pickup + timedelta(minutes=random.randint(15, 60))
        delivery_status = (
            "delivered"
            if status == "delivered"
            else random.choice(["picked_up", "in_transit", "failed"])
        )
        rows.append([
            delivery_id,
            order_id,
            fake.name(),
            fake.phone_number(),
            pickup.isoformat(timespec="seconds"),
            delivered.isoformat(timespec="seconds"),
            delivery_status,
            round(random.uniform(0.5, 12.0), 2),
        ])
        delivery_id += 1
    write_csv(
        "raw_deliveries.csv",
        ["delivery_id", "order_id", "courier_name", "courier_phone", "pickup_timestamp", "delivery_timestamp", "delivery_status", "distance_km"],
        rows,
    )


def main():
    os.makedirs(SEEDS_DIR, exist_ok=True)
    seed_customers()
    seed_restaurants()
    seed_menu_items()
    orders = seed_orders()
    seed_order_items(orders)
    seed_deliveries(orders)
    print("Seed complete.")


if __name__ == "__main__":
    main()