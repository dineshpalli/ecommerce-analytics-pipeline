# Data Dictionary

This document provides detailed descriptions of all data models in the E-Commerce Analytics Pipeline.

---

## Staging Models

### stg_events
**Description**: Cleaned and standardized event data from the raw events source.
**Grain**: One row per event.
**Update Frequency**: Real-time (streaming) or batch (daily).

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| event_id | STRING | Unique identifier for the event | EVT_A1B2C3D4E5F6G7H8 |
| event_type | STRING | Type of user action | page_view, purchase |
| user_id | STRING | Identifier for the user | USER_ABC123DEF456 |
| session_id | STRING | Session identifier | SES_XYZ789QRS012 |
| event_timestamp | TIMESTAMP | When the event occurred (UTC) | 2024-01-15 14:30:00 |
| event_date | DATE | Date of the event | 2024-01-15 |
| event_hour | INT | Hour of the event (0-23) | 14 |
| day_of_week | INT | Day of week (0=Sunday) | 1 |
| is_weekend | BOOLEAN | Whether event occurred on weekend | FALSE |
| device_type | STRING | Device used | mobile, desktop, tablet |
| country_code | STRING | Two-letter country code | US, DE, UK |
| traffic_source | STRING | How user arrived | organic, paid_search |
| product_id | STRING | Product involved (if applicable) | PROD_12345678 |
| product_category | STRING | Product category | Electronics |
| revenue_amount | DECIMAL | Revenue generated (purchases only) | 99.99 |
| is_revenue_event | BOOLEAN | Whether this is a purchase with revenue | TRUE |
| event_properties | JSON | Additional event-specific data | {"cart_value": 150.00} |

**Event Types**:
- `page_view`: User viewed a page
- `product_view`: User viewed a product detail page
- `add_to_cart`: User added item to cart
- `remove_from_cart`: User removed item from cart
- `begin_checkout`: User started checkout process
- `purchase`: User completed a purchase
- `search`: User performed a search
- `signup`: User created an account
- `login`: User logged in

---

### stg_products
**Description**: Cleaned product catalog with derived attributes.
**Grain**: One row per product.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| product_id | STRING | Unique product identifier | PROD_12345678 |
| product_name | STRING | Product display name | Wireless Headphones Pro |
| category | STRING | Product category | Electronics |
| subcategory | STRING | Product subcategory | Audio |
| brand | STRING | Product brand | TechBrand |
| price | DECIMAL | Price in USD | 149.99 |
| price_tier | STRING | Price classification | Premium |
| rating | DECIMAL | Average customer rating (0-5) | 4.5 |
| rating_tier | STRING | Rating classification | Excellent |
| review_count | INT | Number of reviews | 1250 |
| is_in_stock | BOOLEAN | Current availability | TRUE |

**Price Tiers**:
- Budget: < $25
- Mid-Range: $25 - $99
- Premium: $100 - $499
- Luxury: >= $500

---

### stg_users
**Description**: Cleaned user profiles with derived attributes.
**Grain**: One row per user.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| user_id | STRING | Unique user identifier | USER_ABC123DEF456 |
| user_segment | STRING | Behavioral segment | power_buyer |
| preferred_device | STRING | Most used device type | mobile |
| acquisition_source | STRING | How user was acquired | organic |
| country_code | STRING | User's country | US |
| city | STRING | User's city | New York |
| account_created_at | TIMESTAMP | When account was created | 2023-06-15 10:00:00 |
| account_age_days | INT | Days since account creation | 180 |
| tenure_tier | STRING | Account age classification | Established |
| is_email_subscribed | BOOLEAN | Email subscription status | TRUE |

**User Segments**:
- `power_buyer`: High purchase frequency (top 5%)
- `browser`: High engagement, low purchases (30%)
- `occasional`: Moderate engagement (45%)
- `new_user`: Recently joined (20%)

---

## Intermediate Models

### int_sessions
**Description**: Session-level aggregations with engagement metrics.
**Grain**: One row per session.

| Column | Type | Description |
|--------|------|-------------|
| session_id | STRING | Unique session identifier |
| user_id | STRING | User who owned the session |
| session_start | TIMESTAMP | First event timestamp |
| session_end | TIMESTAMP | Last event timestamp |
| session_duration_seconds | INT | Total session length |
| session_date | DATE | Date of session |
| device_type | STRING | Device used |
| country_code | STRING | User's country |
| traffic_source | STRING | Acquisition source |
| total_events | INT | Count of all events |
| page_views | INT | Page view count |
| product_views | INT | Product view count |
| add_to_cart_events | INT | Cart addition count |
| purchases | INT | Purchase count |
| session_revenue | DECIMAL | Total revenue in session |
| duration_category | STRING | Duration classification |
| is_converted | BOOLEAN | Had a purchase |
| engagement_score | DECIMAL | Weighted engagement metric |
| session_quality | STRING | Quality tier |

**Duration Categories**:
- Bounce: < 30 seconds
- Quick: 30s - 2 minutes
- Engaged: 2 - 10 minutes
- Deep: 10 - 30 minutes
- Extended: > 30 minutes

---

### int_user_journey
**Description**: User funnel progression tracking.
**Grain**: One row per user per session per day.

| Column | Type | Description |
|--------|------|-------------|
| user_id | STRING | User identifier |
| session_id | STRING | Session identifier |
| event_date | DATE | Date of activity |
| reached_site | BOOLEAN | Visited the site |
| reached_product | BOOLEAN | Viewed a product |
| reached_cart | BOOLEAN | Added to cart |
| reached_checkout | BOOLEAN | Started checkout |
| reached_purchase | BOOLEAN | Completed purchase |
| deepest_stage | STRING | Furthest funnel stage |
| funnel_depth | INT | Numeric stage (0-5) |
| drop_off_stage | STRING | Where user exited |
| time_to_product_view | INT | Seconds to product view |
| time_to_cart | INT | Seconds to cart addition |
| time_to_purchase | INT | Seconds to purchase |
| total_revenue | DECIMAL | Revenue generated |

---

## Mart Models (Analytics-Ready)

### dim_users
**Description**: Complete user dimension with lifetime metrics and RFM scoring.
**Grain**: One row per user.
**SCD Type**: 1 (overwrite)

| Column | Type | Description |
|--------|------|-------------|
| user_id | STRING | Primary key |
| user_segment | STRING | Behavioral segment |
| preferred_device | STRING | Most used device |
| acquisition_source | STRING | Original traffic source |
| country_code | STRING | Country |
| tenure_tier | STRING | Account age tier |
| lifetime_sessions | INT | Total sessions ever |
| lifetime_revenue | DECIMAL | Total revenue generated |
| lifetime_purchases | INT | Total purchase count |
| recency_days | INT | Days since last activity |
| recency_score | INT | RFM recency (1-5) |
| frequency_score | INT | RFM frequency (1-5) |
| monetary_score | INT | RFM monetary (1-5) |
| rfm_total_score | INT | Combined RFM score |
| customer_value_tier | STRING | Value classification |
| activity_status | STRING | Current engagement status |
| buyer_status | STRING | Purchase history status |

**Customer Value Tiers**:
- VIP: >= $1,000 lifetime
- High Value: $500 - $999
- Medium Value: $100 - $499
- Low Value: $1 - $99
- No Purchase: $0

---

### dim_products
**Description**: Product dimension with performance metrics.
**Grain**: One row per product.

| Column | Type | Description |
|--------|------|-------------|
| product_id | STRING | Primary key |
| product_name | STRING | Display name |
| category | STRING | Category |
| price | DECIMAL | Current price |
| price_tier | STRING | Price classification |
| rating | DECIMAL | Average rating |
| total_views | INT | Lifetime views |
| total_purchases | INT | Lifetime purchases |
| total_revenue | DECIMAL | Lifetime revenue |
| view_to_cart_rate | DECIMAL | View → Cart conversion |
| overall_conversion_rate | DECIMAL | View → Purchase conversion |
| revenue_tier | STRING | Revenue performance |
| product_health | STRING | Overall health indicator |

---

### dim_date
**Description**: Date dimension for time-based analysis.
**Grain**: One row per calendar date.

| Column | Type | Description |
|--------|------|-------------|
| date_key | DATE | Primary key |
| year | INT | Calendar year |
| quarter | INT | Calendar quarter (1-4) |
| month | INT | Month (1-12) |
| month_name | STRING | Full month name |
| week_of_year | INT | Week number |
| day_of_week | INT | Day (0=Sunday) |
| day_name | STRING | Full day name |
| is_weekend | BOOLEAN | Weekend flag |
| is_month_start | BOOLEAN | First day of month |
| is_month_end | BOOLEAN | Last day of month |

---

### fct_daily_engagement
**Description**: Daily KPIs for executive dashboards.
**Grain**: One row per date.

| Column | Type | Description |
|--------|------|-------------|
| event_date | DATE | Primary key |
| daily_active_users | INT | Unique users |
| total_sessions | INT | Session count |
| total_events | INT | Event count |
| page_views | INT | Page view count |
| purchases | INT | Purchase count |
| total_revenue | DECIMAL | Daily revenue |
| conversion_rate | DECIMAL | Session → Purchase rate |
| avg_order_value | DECIMAL | Average purchase value |
| revenue_per_user | DECIMAL | Revenue / DAU |
| dau_7d_avg | DECIMAL | 7-day rolling DAU |
| revenue_7d_avg | DECIMAL | 7-day rolling revenue |
| dau_dod_change | DECIMAL | Day-over-day DAU change |
| dau_wow_change | DECIMAL | Week-over-week DAU change |

---

### fct_funnel
**Description**: Conversion funnel metrics.
**Grain**: One row per date.

| Column | Type | Description |
|--------|------|-------------|
| event_date | DATE | Primary key |
| users_at_site | INT | Users who visited |
| users_at_product | INT | Users who viewed products |
| users_at_cart | INT | Users who added to cart |
| users_at_checkout | INT | Users who started checkout |
| users_at_purchase | INT | Users who purchased |
| site_to_product_rate | DECIMAL | Stage conversion rate |
| product_to_cart_rate | DECIMAL | Stage conversion rate |
| cart_to_checkout_rate | DECIMAL | Stage conversion rate |
| checkout_to_purchase_rate | DECIMAL | Stage conversion rate |
| overall_conversion_rate | DECIMAL | End-to-end rate |
| bounce_rate | DECIMAL | Single-page sessions |
| cart_abandonment_rate | DECIMAL | Cart → No purchase rate |

---

### fct_revenue
**Description**: Revenue analytics by category.
**Grain**: One row per date × category.

| Column | Type | Description |
|--------|------|-------------|
| event_date | DATE | Transaction date |
| category | STRING | Product category |
| transaction_count | INT | Number of transactions |
| purchasing_users | INT | Unique purchasers |
| gross_revenue | DECIMAL | Total revenue |
| avg_transaction_value | DECIMAL | Average order value |
| mobile_revenue | DECIMAL | Revenue from mobile |
| desktop_revenue | DECIMAL | Revenue from desktop |
| organic_revenue | DECIMAL | Revenue from organic |
| paid_revenue | DECIMAL | Revenue from paid |
| category_revenue_share | DECIMAL | % of daily total |
| revenue_7d_rolling | DECIMAL | 7-day rolling sum |
| revenue_wow_growth | DECIMAL | Week-over-week growth |
