"""
generate_dataset.py
-------------------
Generates a realistic synthetic sales operations dataset.
Run this script once to create data/raw/sales_operations.csv

Usage:
    python generate_dataset.py
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

random.seed(42)
np.random.seed(42)

# ── Configuration ───────────────────────────────────────────────────────────
START_DATE = datetime(2022, 1, 1)
END_DATE   = datetime(2024, 12, 31)
N_ROWS     = 10_800

PRODUCTS = {
    "Electronics": [
        ("Laptop Pro 15",      1200, 700),
        ("Wireless Headset",    180,  60),
        ("4K Monitor",          450, 210),
        ("Docking Station",     280, 110),
        ("Webcam HD",            90,  35),
    ],
    "Software": [
        ("CRM Suite - Annual",  2400, 300),
        ("Analytics Platform",  3600, 420),
        ("Security Bundle",     1800, 180),
        ("Collaboration Tools",  960, 120),
        ("ERP Module",          6000, 800),
    ],
    "Services": [
        ("Implementation",      8000, 2000),
        ("Training Package",    1500,  300),
        ("Support Contract",    2000,  250),
        ("Consulting Day Rate", 1200,  400),
        ("Data Migration",      4500, 1200),
    ],
    "Hardware": [
        ("Server Rack Unit",    5500, 3200),
        ("Network Switch 48p",  1800,  900),
        ("UPS Battery System",   950,  500),
        ("Firewall Appliance",  3200, 1600),
        ("Storage Array 10TB",  7800, 4200),
    ],
}

REGIONS = {
    "North":         0.22,
    "South":         0.18,
    "East":          0.25,
    "West":          0.20,
    "International": 0.15,
}

CHANNELS = {
    "Direct":  0.45,
    "Partner": 0.35,
    "Online":  0.20,
}

CUSTOMER_SEGMENTS = {
    "Enterprise": 0.40,
    "SMB":        0.35,
    "Consumer":   0.25,
}

SALES_REPS = [
    "Alex Johnson", "Maria Garcia", "David Chen", "Sarah Williams",
    "James Martinez", "Emily Brown", "Michael Davis", "Jessica Wilson",
    "Robert Anderson", "Amanda Taylor", "Christopher Thomas", "Stephanie Jackson",
]

def generate_customer_id():
    return f"CUST-{random.randint(1000, 9999)}"

def seasonal_multiplier(date: datetime) -> float:
    """Q4 strong, Q1 slow - typical B2B seasonality."""
    q = (date.month - 1) // 3 + 1
    return {1: 0.82, 2: 0.95, 3: 1.08, 4: 1.25}[q]

def yoy_growth_factor(date: datetime) -> float:
    """Simulate YoY revenue growth: 2022 base, 2023 +12%, 2024 +18%."""
    return {2022: 1.00, 2023: 1.12, 2024: 1.18}[date.year]

def generate_row(i: int) -> dict:
    # Random date
    days_range = (END_DATE - START_DATE).days
    date = START_DATE + timedelta(days=random.randint(0, days_range))

    # Product
    category = random.choices(list(PRODUCTS.keys()), weights=[0.20, 0.35, 0.30, 0.15])[0]
    product_name, base_price, base_cost = random.choice(PRODUCTS[category])

    # Units
    units = random.choices([1, 2, 3, 5, 10], weights=[0.50, 0.25, 0.12, 0.08, 0.05])[0]

    # Discount (Enterprise gets better discounts)
    segment = random.choices(list(CUSTOMER_SEGMENTS.keys()),
                             weights=list(CUSTOMER_SEGMENTS.values()))[0]
    max_disc = {"Enterprise": 0.35, "SMB": 0.20, "Consumer": 0.10}[segment]
    discount_pct = round(random.uniform(0.0, max_disc), 4)

    # Revenue and cost with seasonality + growth
    growth = yoy_growth_factor(date)
    season = seasonal_multiplier(date)
    noise  = np.random.normal(1.0, 0.08)

    raw_revenue = base_price * units * growth * season * noise
    revenue     = round(raw_revenue * (1 - discount_pct), 2)
    cost        = round(base_cost * units * np.random.normal(1.0, 0.05), 2)

    # Inject ~2% anomalies for detection demo
    if random.random() < 0.02:
        revenue *= random.choice([3.5, 0.1, 4.2])
        revenue = round(revenue, 2)

    region  = random.choices(list(REGIONS.keys()),  weights=list(REGIONS.values()))[0]
    channel = random.choices(list(CHANNELS.keys()), weights=list(CHANNELS.values()))[0]
    rep     = random.choice(SALES_REPS)

    return {
        "transaction_id":   f"TXN-{i+1:06d}",
        "date":             date.strftime("%Y-%m-%d"),
        "product_name":     product_name,
        "product_category": category,
        "region":           region,
        "sales_rep":        rep,
        "customer_id":      generate_customer_id(),
        "customer_segment": segment,
        "channel":          channel,
        "units_sold":       units,
        "unit_price":       base_price,
        "revenue":          revenue,
        "cost":             cost,
        "discount_pct":     round(discount_pct * 100, 2),  # stored as %
    }

def main():
    print(f"Generating {N_ROWS:,} rows of sales data...")
    rows = [generate_row(i) for i in range(N_ROWS)]
    df   = pd.DataFrame(rows)

    # Sort by date
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    os.makedirs("data/raw", exist_ok=True)
    out = "data/raw/sales_operations.csv"
    df.to_csv(out, index=False)

    print(f"OK Saved {len(df):,} rows - {out}")
    print(f"\nColumn summary:")
    print(df.dtypes.to_string())
    print(f"\nRevenue range: ${df['revenue'].min():,.2f} - ${df['revenue'].max():,.2f}")
    print(f"Date range:    {df['date'].min()} - {df['date'].max()}")
    print(f"Regions:       {sorted(df['region'].unique())}")
    print(f"Categories:    {sorted(df['product_category'].unique())}")

if __name__ == "__main__":
    main()
