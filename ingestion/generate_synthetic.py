import os
import random
from datetime import timedelta
import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)

# Ensure data directory exists
os.makedirs("data", exist_ok=True)

cities = ["Dubai", "Abu Dhabi", "Sharjah"]
channels = ["app", "web", "call_center"]
worker_types = ["live_in", "live_out"]


def gen_customers(n=500):
    rows = []
    for i in range(1, n + 1):
        cid = f"C{i:05d}"
        created = fake.date_time_between(start_date="-365d", end_date="-180d")
        updated = created + timedelta(days=random.randint(0, 180))
        rows.append({
            "customer_id": cid,
            "full_name": fake.name(),
            "email": fake.email(),
            "phone": fake.msisdn()[:12],
            "city": random.choice(cities),
            "created_at": created,
            "updated_at": updated
        })
    return pd.DataFrame(rows)


def gen_workers(n=200):
    rows = []
    for i in range(1, n + 1):
        wid = f"W{i:05d}"
        created = fake.date_time_between(start_date="-365d", end_date="-300d")
        updated = created + timedelta(days=random.randint(0, 300))
        rows.append({
            "worker_id": wid,
            "worker_name": fake.name(),
            "worker_type": random.choice(worker_types),
            "city": random.choice(cities),
            "is_active": random.random() > 0.1,
            "created_at": created,
            "updated_at": updated
        })
    return pd.DataFrame(rows)


def gen_bookings(customers_df: pd.DataFrame, workers_df: pd.DataFrame, n=5000):
    rows = []
    for i in range(1, n + 1):
        bid = f"B{i:06d}"
        c = customers_df.sample(1).iloc[0]
        w = workers_df.sample(1).iloc[0]
        city = random.choice([c["city"], w["city"]])
        channel = random.choice(channels)
        requested = fake.date_time_between(start_date="-180d", end_date="now")

        # Assignment happens within 0–8 hours after request
        assigned_delay = random.randint(0, 8 * 60)  # minutes
        assigned = requested + timedelta(minutes=assigned_delay)

        # Outcome and completion/cancel timing
        status = random.choices(
            ["completed", "canceled", "pending"],
            weights=[0.7, 0.2, 0.1]
        )[0]
        if status == "completed":
            # Completion 1–8 hours after assignment
            completed_delay = random.randint(60, 8 * 60)
            completed = assigned + timedelta(minutes=completed_delay)
            canceled = None
        elif status == "canceled":
            # Cancellation within 5–120 minutes after assignment
            completed = None
            canceled = assigned + timedelta(minutes=random.randint(5, 120))
        else:
            completed = None
            canceled = None

        price = round(random.uniform(80, 400), 2)
        # Simulate late updates (up to 3 days after request)
        updated = requested + timedelta(minutes=random.randint(0, 3 * 24 * 60))

        rows.append({
            "booking_id": bid,
            "customer_id": c["customer_id"],
            "worker_id": w["worker_id"],
            "city": city,
            "channel": channel,
            "status": status,
            "price": price,
            "requested_at": requested,
            "assigned_at": assigned,
            "completed_at": completed,
            "canceled_at": canceled,
            "updated_at": updated
        })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    customers = gen_customers(500)
    workers = gen_workers(200)
    bookings = gen_bookings(customers, workers, 5000)

    customers.to_csv("data/customers.csv", index=False)
    workers.to_csv("data/workers.csv", index=False)
    bookings.to_csv("data/bookings.csv", index=False)

    print("Generated:")
    print(f"  data/customers.csv  ({len(customers)} rows)")
    print(f"  data/workers.csv    ({len(workers)} rows)")
    print(f"  data/bookings.csv   ({len(bookings)} rows)")
