#!/usr/bin/env python3
"""
Synthetic E-Commerce Event Data Generator

Generates realistic user behavior events for an e-commerce platform.
Supports multiple event types with configurable volume and time range.

Usage:
    python generate_events.py --events 100000 --days 30
    python generate_events.py --events 50000 --days 7 --seed 42
"""

import argparse
import hashlib
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker

# Initialize Faker with seed for reproducibility
fake = Faker()


class EventType:
    """Event type constants with their probabilities."""

    PAGE_VIEW = "page_view"
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    BEGIN_CHECKOUT = "begin_checkout"
    PURCHASE = "purchase"
    SEARCH = "search"
    SIGNUP = "signup"
    LOGIN = "login"

    # Event probabilities (should sum to 1.0)
    PROBABILITIES = {
        PAGE_VIEW: 0.30,
        PRODUCT_VIEW: 0.25,
        ADD_TO_CART: 0.15,
        REMOVE_FROM_CART: 0.05,
        BEGIN_CHECKOUT: 0.08,
        PURCHASE: 0.05,
        SEARCH: 0.07,
        SIGNUP: 0.02,
        LOGIN: 0.03,
    }


class ProductCatalog:
    """Simulated product catalog with categories and pricing."""

    CATEGORIES = [
        "Electronics",
        "Clothing",
        "Home & Garden",
        "Sports & Outdoors",
        "Books",
        "Beauty & Personal Care",
        "Toys & Games",
        "Food & Beverages",
    ]

    PRODUCTS_PER_CATEGORY = 50

    def __init__(self, seed: int | None = None):
        if seed:
            random.seed(seed)
            np.random.seed(seed)
            Faker.seed(seed)

        self.products = self._generate_catalog()

    def _generate_catalog(self) -> list[dict[str, Any]]:
        """Generate a product catalog with realistic attributes."""
        products = []

        for category in self.CATEGORIES:
            for i in range(self.PRODUCTS_PER_CATEGORY):
                product_id = f"PROD_{hashlib.md5(f'{category}_{i}'.encode()).hexdigest()[:8].upper()}"

                # Price varies by category
                base_price = {
                    "Electronics": (50, 2000),
                    "Clothing": (15, 300),
                    "Home & Garden": (10, 500),
                    "Sports & Outdoors": (20, 800),
                    "Books": (5, 50),
                    "Beauty & Personal Care": (5, 150),
                    "Toys & Games": (10, 200),
                    "Food & Beverages": (2, 100),
                }[category]

                price = round(random.uniform(*base_price), 2)

                products.append(
                    {
                        "product_id": product_id,
                        "product_name": fake.catch_phrase(),
                        "category": category,
                        "subcategory": fake.word().capitalize(),
                        "price": price,
                        "brand": fake.company(),
                        "rating": round(random.uniform(1.0, 5.0), 1),
                        "review_count": random.randint(0, 5000),
                        "in_stock": random.random() > 0.1,  # 90% in stock
                    }
                )

        return products

    def get_random_product(self) -> dict[str, Any]:
        """Get a random product from the catalog."""
        return random.choice(self.products)

    def get_products_by_category(self, category: str) -> list[dict[str, Any]]:
        """Get all products in a category."""
        return [p for p in self.products if p["category"] == category]

    def to_dataframe(self) -> pd.DataFrame:
        """Export catalog as DataFrame."""
        return pd.DataFrame(self.products)


class UserPool:
    """Simulated user pool with behavioral patterns."""

    def __init__(self, num_users: int = 10000, seed: int | None = None):
        if seed:
            random.seed(seed)
            np.random.seed(seed)
            Faker.seed(seed)

        self.users = self._generate_users(num_users)

    def _generate_users(self, num_users: int) -> list[dict[str, Any]]:
        """Generate user profiles with behavioral attributes."""
        users = []

        # User segments with different behaviors
        segments = [
            ("power_buyer", 0.05),  # 5% - frequent purchasers
            ("browser", 0.30),  # 30% - lots of views, few purchases
            ("occasional", 0.45),  # 45% - occasional engagement
            ("new_user", 0.20),  # 20% - recently joined
        ]

        for i in range(num_users):
            user_id = f"USER_{uuid.uuid4().hex[:12].upper()}"

            # Assign segment based on probabilities
            segment = np.random.choice(
                [s[0] for s in segments], p=[s[1] for s in segments]
            )

            # Device preferences
            devices = ["mobile", "desktop", "tablet"]
            device_probs = {
                "power_buyer": [0.3, 0.6, 0.1],
                "browser": [0.6, 0.3, 0.1],
                "occasional": [0.5, 0.4, 0.1],
                "new_user": [0.7, 0.2, 0.1],
            }

            primary_device = np.random.choice(devices, p=device_probs[segment])

            # Traffic source
            sources = ["organic", "paid_search", "social", "email", "direct", "referral"]
            source_probs = {
                "power_buyer": [0.3, 0.2, 0.1, 0.15, 0.2, 0.05],
                "browser": [0.2, 0.3, 0.25, 0.1, 0.1, 0.05],
                "occasional": [0.25, 0.25, 0.2, 0.1, 0.15, 0.05],
                "new_user": [0.15, 0.35, 0.3, 0.05, 0.1, 0.05],
            }

            traffic_source = np.random.choice(sources, p=source_probs[segment])

            # Geographic distribution
            countries = ["US", "UK", "DE", "FR", "CA", "AU", "NL", "ES", "IT", "BR"]
            country_probs = [0.40, 0.15, 0.10, 0.08, 0.07, 0.05, 0.05, 0.04, 0.03, 0.03]

            users.append(
                {
                    "user_id": user_id,
                    "email": fake.email(),
                    "segment": segment,
                    "primary_device": primary_device,
                    "traffic_source": traffic_source,
                    "country": np.random.choice(countries, p=country_probs),
                    "city": fake.city(),
                    "created_at": fake.date_time_between(
                        start_date="-2y", end_date="now"
                    ),
                    "is_subscribed": random.random() > 0.6,
                    "lifetime_value": 0.0,  # Will be calculated from events
                }
            )

        return users

    def get_random_user(self) -> dict[str, Any]:
        """Get a random user, weighted by activity level."""
        # Power buyers are more likely to appear in events
        weights = [
            (
                3.0
                if u["segment"] == "power_buyer"
                else 1.5 if u["segment"] == "browser" else 1.0
            )
            for u in self.users
        ]
        weights = np.array(weights) / sum(weights)
        return np.random.choice(self.users, p=weights)

    def to_dataframe(self) -> pd.DataFrame:
        """Export users as DataFrame."""
        return pd.DataFrame(self.users)


class EventGenerator:
    """
    Main event generator with realistic user behavior simulation.

    Generates events that follow realistic patterns:
    - Users have sessions with multiple events
    - Conversion funnels follow typical patterns
    - Time distribution follows hourly/daily patterns
    """

    def __init__(
        self,
        num_events: int,
        num_days: int,
        num_users: int = 10000,
        seed: int | None = None,
    ):
        self.num_events = num_events
        self.num_days = num_days
        self.seed = seed

        if seed:
            random.seed(seed)
            np.random.seed(seed)

        self.catalog = ProductCatalog(seed=seed)
        self.user_pool = UserPool(num_users=num_users, seed=seed)

        # Track user sessions
        self.active_sessions: dict[str, dict] = {}
        self.session_timeout_minutes = 30

    def _generate_timestamp(self, start_date: datetime, end_date: datetime) -> datetime:
        """Generate timestamp with realistic hourly/daily patterns."""
        # Random date within range
        days_range = (end_date - start_date).days
        random_day = start_date + timedelta(days=random.randint(0, days_range))

        # Hour distribution (more activity during business hours and evening)
        hour_weights = [
            0.01,
            0.005,
            0.005,
            0.005,
            0.01,
            0.02,  # 0-5 AM
            0.03,
            0.05,
            0.07,
            0.08,
            0.08,
            0.08,  # 6-11 AM
            0.07,
            0.06,
            0.06,
            0.06,
            0.07,
            0.08,  # 12-5 PM
            0.09,
            0.10,
            0.08,
            0.06,
            0.04,
            0.02,  # 6-11 PM
        ]

        # Weekend has different pattern (more midday activity)
        if random_day.weekday() >= 5:
            hour_weights = [
                0.01,
                0.01,
                0.01,
                0.01,
                0.01,
                0.02,  # 0-5 AM
                0.02,
                0.04,
                0.06,
                0.08,
                0.09,
                0.10,  # 6-11 AM
                0.10,
                0.09,
                0.08,
                0.07,
                0.06,
                0.06,  # 12-5 PM
                0.05,
                0.05,
                0.04,
                0.03,
                0.02,
                0.01,  # 6-11 PM
            ]

        hour_weights = np.array(hour_weights) / sum(hour_weights)
        hour = np.random.choice(range(24), p=hour_weights)

        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        microsecond = random.randint(0, 999999)

        return random_day.replace(
            hour=hour, minute=minute, second=second, microsecond=microsecond
        )

    def _get_or_create_session(
        self, user: dict[str, Any], timestamp: datetime
    ) -> str:
        """Get existing session or create new one."""
        user_id = user["user_id"]

        if user_id in self.active_sessions:
            session = self.active_sessions[user_id]
            last_activity = session["last_activity"]

            # Check if session is still active
            if (timestamp - last_activity).total_seconds() < self.session_timeout_minutes * 60:
                session["last_activity"] = timestamp
                session["event_count"] += 1
                return session["session_id"]

        # Create new session
        session_id = f"SES_{uuid.uuid4().hex[:16].upper()}"
        self.active_sessions[user_id] = {
            "session_id": session_id,
            "start_time": timestamp,
            "last_activity": timestamp,
            "event_count": 1,
            "cart": [],
        }

        return session_id

    def _generate_event_properties(
        self, event_type: str, user: dict[str, Any], session_id: str
    ) -> dict[str, Any]:
        """Generate event-specific properties."""
        properties: dict[str, Any] = {
            "device": user["primary_device"],
            "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
            "os": random.choice(["Windows", "macOS", "iOS", "Android", "Linux"]),
            "country": user["country"],
            "traffic_source": user["traffic_source"],
        }

        if event_type == EventType.PAGE_VIEW:
            pages = [
                "/",
                "/products",
                "/categories",
                "/about",
                "/contact",
                "/faq",
                "/blog",
            ]
            properties["page_path"] = random.choice(pages)
            properties["page_title"] = fake.sentence(nb_words=4)
            properties["referrer"] = (
                fake.url() if random.random() > 0.3 else None
            )

        elif event_type == EventType.PRODUCT_VIEW:
            product = self.catalog.get_random_product()
            properties["product_id"] = product["product_id"]
            properties["product_name"] = product["product_name"]
            properties["category"] = product["category"]
            properties["price"] = product["price"]
            properties["page_path"] = f"/products/{product['product_id']}"

        elif event_type == EventType.ADD_TO_CART:
            product = self.catalog.get_random_product()
            quantity = np.random.choice([1, 2, 3, 4, 5], p=[0.6, 0.25, 0.1, 0.03, 0.02])
            properties["product_id"] = product["product_id"]
            properties["product_name"] = product["product_name"]
            properties["category"] = product["category"]
            properties["price"] = product["price"]
            properties["quantity"] = quantity
            properties["cart_value"] = round(product["price"] * quantity, 2)

            # Track in session cart
            if user["user_id"] in self.active_sessions:
                self.active_sessions[user["user_id"]]["cart"].append(
                    {"product": product, "quantity": quantity}
                )

        elif event_type == EventType.REMOVE_FROM_CART:
            product = self.catalog.get_random_product()
            properties["product_id"] = product["product_id"]
            properties["product_name"] = product["product_name"]
            properties["price"] = product["price"]

        elif event_type == EventType.BEGIN_CHECKOUT:
            # Use session cart if available
            cart_items = random.randint(1, 5)
            cart_value = sum(
                self.catalog.get_random_product()["price"] for _ in range(cart_items)
            )
            properties["cart_items"] = cart_items
            properties["cart_value"] = round(cart_value, 2)
            properties["checkout_step"] = "shipping"

        elif event_type == EventType.PURCHASE:
            # Purchase event with order details
            order_items = random.randint(1, 5)
            products = [self.catalog.get_random_product() for _ in range(order_items)]
            subtotal = sum(p["price"] for p in products)
            tax = round(subtotal * 0.08, 2)  # 8% tax
            shipping = round(random.uniform(0, 15), 2)
            total = round(subtotal + tax + shipping, 2)

            properties["order_id"] = f"ORD_{uuid.uuid4().hex[:12].upper()}"
            properties["items"] = [
                {
                    "product_id": p["product_id"],
                    "product_name": p["product_name"],
                    "category": p["category"],
                    "price": p["price"],
                    "quantity": 1,
                }
                for p in products
            ]
            properties["item_count"] = order_items
            properties["subtotal"] = round(subtotal, 2)
            properties["tax"] = tax
            properties["shipping"] = shipping
            properties["total"] = total
            properties["payment_method"] = random.choice(
                ["credit_card", "paypal", "apple_pay", "google_pay"]
            )
            properties["currency"] = "USD"

        elif event_type == EventType.SEARCH:
            search_terms = [
                "laptop",
                "shoes",
                "headphones",
                "dress",
                "watch",
                "camera",
                "book",
                "coffee maker",
                "yoga mat",
                "backpack",
            ]
            properties["search_query"] = random.choice(search_terms)
            properties["results_count"] = random.randint(0, 500)
            properties["search_type"] = random.choice(["text", "voice", "image"])

        elif event_type == EventType.SIGNUP:
            properties["signup_source"] = random.choice(
                ["organic", "paid", "referral", "social"]
            )
            properties["has_subscribed"] = random.random() > 0.4

        elif event_type == EventType.LOGIN:
            properties["login_method"] = random.choice(
                ["email", "google", "facebook", "apple"]
            )
            properties["is_returning"] = random.random() > 0.3

        return properties

    def generate(self) -> pd.DataFrame:
        """Generate all events and return as DataFrame."""
        print(f"Generating {self.num_events:,} events over {self.num_days} days...")

        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.num_days)

        events = []
        event_types = list(EventType.PROBABILITIES.keys())
        event_probs = list(EventType.PROBABILITIES.values())

        for i in range(self.num_events):
            if (i + 1) % 10000 == 0:
                print(f"  Generated {i + 1:,} events...")

            # Select user and event type
            user = self.user_pool.get_random_user()
            event_type = np.random.choice(event_types, p=event_probs)

            # Adjust probabilities based on user segment
            if user["segment"] == "power_buyer" and event_type in [
                EventType.PAGE_VIEW,
                EventType.PRODUCT_VIEW,
            ]:
                # Power buyers are more likely to convert
                if random.random() > 0.6:
                    event_type = random.choice(
                        [EventType.ADD_TO_CART, EventType.PURCHASE]
                    )

            # Generate timestamp and session
            timestamp = self._generate_timestamp(start_date, end_date)
            session_id = self._get_or_create_session(user, timestamp)

            # Generate event properties
            properties = self._generate_event_properties(event_type, user, session_id)

            event = {
                "event_id": f"EVT_{uuid.uuid4().hex[:16].upper()}",
                "event_type": event_type,
                "user_id": user["user_id"],
                "session_id": session_id,
                "timestamp": timestamp.isoformat(),
                "properties": json.dumps(properties),
                # Flattened common properties for easier querying
                "device": properties.get("device"),
                "country": properties.get("country"),
                "traffic_source": properties.get("traffic_source"),
                # Event-specific flattened fields
                "product_id": properties.get("product_id"),
                "category": properties.get("category"),
                "revenue": properties.get("total", 0.0),
            }

            events.append(event)

        # Sort by timestamp
        df = pd.DataFrame(events)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)

        print(f"Generated {len(df):,} events successfully!")

        return df

    def save_reference_data(self, output_dir: Path) -> None:
        """Save product catalog and user data as reference files."""
        # Save product catalog
        products_df = self.catalog.to_dataframe()
        products_path = output_dir / "products.parquet"
        products_df.to_parquet(products_path, index=False)
        print(f"Saved product catalog: {products_path}")

        # Save user profiles (without PII for privacy)
        users_df = self.user_pool.to_dataframe()
        users_df = users_df.drop(columns=["email"])  # Remove PII
        users_path = output_dir / "users.parquet"
        users_df.to_parquet(users_path, index=False)
        print(f"Saved user profiles: {users_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic e-commerce event data"
    )
    parser.add_argument(
        "--events",
        type=int,
        default=100000,
        help="Number of events to generate (default: 100000)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to span (default: 30)",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=10000,
        help="Number of users in the pool (default: 10000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output directory (default: data/raw)",
    )

    args = parser.parse_args()

    # Determine output directory
    script_dir = Path(__file__).parent.parent
    output_dir = Path(args.output) if args.output else script_dir / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate events
    generator = EventGenerator(
        num_events=args.events,
        num_days=args.days,
        num_users=args.users,
        seed=args.seed,
    )

    events_df = generator.generate()

    # Save events
    date_str = datetime.now().strftime("%Y%m%d")
    events_path = output_dir / f"events_{date_str}.parquet"
    events_df.to_parquet(events_path, index=False)
    print(f"\nSaved events: {events_path}")

    # Save reference data
    seeds_dir = script_dir / "data" / "seeds"
    seeds_dir.mkdir(parents=True, exist_ok=True)
    generator.save_reference_data(seeds_dir)

    # Print summary statistics
    print("\n" + "=" * 60)
    print("GENERATION SUMMARY")
    print("=" * 60)
    print(f"Total events:     {len(events_df):,}")
    print(f"Unique users:     {events_df['user_id'].nunique():,}")
    print(f"Unique sessions:  {events_df['session_id'].nunique():,}")
    print(f"Date range:       {events_df['timestamp'].min()} to {events_df['timestamp'].max()}")
    print(f"\nEvent type distribution:")
    for event_type, count in events_df["event_type"].value_counts().items():
        pct = count / len(events_df) * 100
        print(f"  {event_type:20s}: {count:>8,} ({pct:5.1f}%)")

    print(f"\nTotal revenue:    ${events_df['revenue'].sum():,.2f}")
    print(f"Output files:")
    print(f"  - {events_path}")
    print(f"  - {seeds_dir / 'products.parquet'}")
    print(f"  - {seeds_dir / 'users.parquet'}")


if __name__ == "__main__":
    main()
