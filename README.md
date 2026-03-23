# Practical Assignment: Build Your Own dbt Project for a Business

## Goal

Build a complete **dbt project** for a business scenario of your choice using **DuckDB** as the warehouse.

You must design and implement a small analytical platform using **CSV files in `seeds/`** as the source data, transform the data through **raw**, **stage**, and **mart** layers, and produce business-ready insights.

Your solution must be structured, tested, and explained using correct **dbt** and **data warehousing** terminology.

---

## Business Context

Choose any business domain, for example:

- e-commerce
- food delivery
- hospital
- school
- banking
- streaming platform
- logistics
- hotel booking
- fitness club
- marketplace

You must clearly describe:

- what business you selected
- what data entities exist
- what business questions your project answers

---

## Technical Requirements

Your project must satisfy **all** requirements below.

### 1. Use DuckDB
- The project must run on **DuckDB**.
- Configure the project correctly so that models can be built locally.

### 2. Source Data in Seeds
- All initial data must be stored as **CSV files** inside the `seeds/` folder.
- Use enough source tables to support your business scenario.

### 3. Minimum 20 dbt Models
Create **at least 20 models** in total.

Your project must include the following layers:

- **raw layer**
- **stage layer**
- **mart layer**

You may create more than 20 models.

### 4. At Least 5 Incremental Models
You must create **at least 5 incremental models**.

These models should demonstrate that you understand:

- incremental materialization
- unique keys
- filtering new or changed records
- incremental logic in dbt

### 5. Add Incremental Predicate Logic
At least one incremental model must include an **incremental predicate**.

You must be able to explain:

- why this predicate is needed
- what problem it solves
- what tradeoff it introduces

### 6. Add at Least 1 Custom Macro (Optional + 2.5)
Create **at least 1 macro**.

The macro must be used in your project in a meaningful way.  
Examples:

- standardize text fields
- generate surrogate keys
- calculate business status labels
- reusable date filtering logic
- reusable null handling logic

You must explain:

- what the macro does
- why a macro is better than copying SQL repeatedly

### 7. Use Window Functions
You must use **window functions** in at least **2 models**.

Examples:

- `row_number()`
- `rank()`
- `dense_rank()`
- `lag()`
- `lead()`
- `sum() over (...)`
- `avg() over (...)`

Use them for a real business purpose such as:

- ranking customers
- identifying latest orders
- calculating running totals
- detecting status changes
- measuring customer retention

### 8. Add Tests
Add dbt tests to validate your models.

Include both:

- **generic tests**
- **custom or singular tests** if appropriate

At minimum, test:

- primary key uniqueness
- not null constraints
- accepted values
- relationships between tables

### 9. Follow a Style Guide
Your project must follow a consistent SQL and dbt style guide.

At minimum:

- meaningful model names
- one purpose per model
- clear CTE names
- consistent formatting
- aliases for business-readable column names
- comments or descriptions where useful
- organized folder structure
- YAML documentation for models

### 10. Provide Data Insights
Create a short analytical section that shows **useful business insights** from your marts.

Examples:

- top customers by revenue
- most profitable product category
- repeat purchase behavior
- monthly revenue trend
- churned users
- delayed deliveries
- best-performing city or region

The insights must come from your transformed models, not from raw seed files directly.

### 11. Explain Your Solution
Be prepared to explain your solution using correct terminology, including:

- seed
- source
- model
- materialization
- incremental model
- macro
- test
- staging layer
- mart layer
- window function
- surrogate key
- grain
- lineage
- dependency
- data quality

---

## Assignment Evaluation

| Component | Description | Points |
|---|---|---|
| DuckDB Configuration | Project runs locally using DuckDB and is correctly configured. | 0.5 |
| Seeds (CSV Source Data) | Source data is stored in `seeds/` as CSV files and supports the chosen business scenario. | 0.5 |
| Project Architecture | Clear layered structure: **raw**, **stage**, and **mart** layers with logical dependencies between models. | 1 |
| Minimum 20 dbt Models | At least **20 dbt models** implemented with meaningful transformations. | 1 |
| Incremental Models | At least **5 incremental models** implemented with correct incremental logic and unique keys. | 1 |
| Incremental Predicate | At least **one incremental predicate** implemented and correctly explained. | 0.5 |
| Window Functions | Window functions used in at least **2 models** for meaningful analytical purposes. | 0.5 |
| Data Tests | dbt tests implemented (e.g., `not_null`, `unique`, `relationships`, `accepted_values`). | 0.5 |
| Style Guide and Project Organization | Consistent SQL formatting, meaningful model names, clear CTE structure, proper folder organization, and YAML documentation. | 0.5 |
| Business Insights | Analytical outputs from mart models that provide useful business insights. | 0.5 |
| Solution Explanation | Student can clearly explain the architecture, model grain, transformations, and dbt concepts using correct terminology. | 0.5 |
| **Optional: Custom Macro** | At least one meaningful macro created and used in the project. | **+2.5** |
| **Theoretical Questions** | Answers to the **10 theoretical dbt questions** demonstrating understanding of key concepts. | **3** |
| **Total** |  | **10 (+2.5 optional bonus)** |

---

## Solution: Food Delivery dbt Project

### Business choice

The chosen business domain is a **food-delivery marketplace** that connects customers, restaurants, and couriers. Six entities cover the day-to-day operation:

| Entity | What it represents |
|---|---|
| `customers` | Registered platform users who place orders |
| `restaurants` | Merchants that prepare food and accept orders |
| `menu_items` | Dishes belonging to a restaurant |
| `orders` | A customer's purchase from a single restaurant |
| `order_items` | The line items inside an order |
| `deliveries` | Courier handoff from restaurant to customer |

#### Business questions answered by the marts

- Who are the top-performing restaurants by lifetime revenue?
- Which restaurant leads on each individual day?
- Which couriers deliver fastest, and how do they compare to the platform average?
- How is daily revenue trending and what is the running total?
- Which menu items are gaining or losing momentum day over day?
- How is the customer base growing (new signups + cumulative)?
- What is each customer's rolling 30-day average spend?

### Requirement self-assessment

| # | Requirement | How this project meets it |
|---|---|---|
| 1 | DuckDB | `profiles.yml` configures `dbt-duckdb`; warehouse is `food_delivery.duckdb` in the project root |
| 2 | Source data in `seeds/` | 6 CSVs (`raw_customers`, `raw_restaurants`, `raw_menu_items`, `raw_orders`, `raw_order_items`, `raw_deliveries`) generated by `seed_generator.py` |
| 3 | ≥20 dbt models | **20 models**: 6 staging + 4 dim + 5 fct + 5 rpt |
| 4 | ≥5 incremental models | All 5 facts are incremental: `fct_orders`, `fct_order_items`, `fct_deliveries`, `fct_daily_revenue`, `fct_restaurant_daily` |
| 5 | Incremental predicate | `fct_restaurant_daily` uses `incremental_predicates=["DBT_INCREMENTAL_TARGET.revenue_date >= current_date - interval '7 days'"]` |
| 6 | Custom macro (+2.5) | `macros/standardize_text.sql` — `nullif(trim(lower(col)), '')`, used across all 6 staging models |
| 7 | Window functions in ≥2 models | Used in **5 models**: `fct_daily_revenue`, `rpt_restaurant_daily_rank`, `rpt_customer_30_day_avg_spend`, `rpt_menu_item_sales_momentum`, `rpt_daily_new_customers` |
| 8 | Tests | Generic (`unique`, `not_null`, `relationships`, `accepted_values`) in YAML + 4 singular SQL tests in `tests/marts/` — **137 total checks pass** |
| 9 | Style guide | Layered folder structure, snake_case names, CTE-driven SQL, `_staging__models.yml` / `_marts__models.yml` / `_reports__models.yml` documentation |
| 10 | Business insights | See *Business Insights* section below — 7 SQL queries reading from marts |
| 11 | Solution explanation | See *Theoretical Questions about dbt* (answered inline below) |

---

## Project Setup

### Prerequisites

- Python **3.13** (3.14 breaks `mashumaro`, a transitive dep of `dbt-core`)
- `dbt-duckdb` 1.10+
- (Optional) DuckDB CLI for ad-hoc queries: `brew install duckdb`

### 1. Create virtual environment and install dependencies
```bash
cd food_delivery_dbt
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
Installs `dbt-duckdb` (pipeline runtime) and `faker` (only needed if you regenerate seeds in step 2).

### 2. (Optional) Regenerate seed CSVs
The 6 raw CSVs are committed under `seeds/`, so fresh clones can skip this step and go straight to `dbt build`. Run it only if you want to reproduce or modify the synthetic dataset:
```bash
python seed_generator.py
```
Faker-driven; uses fixed seed `42` for reproducibility.

### 3. Verify the connection
```bash
dbt debug
```

### 4. Build everything
```bash
dbt build       # seed → run → test in DAG order
```
Expected outcome on a clean run: **PASS=137, ERROR=0**.

### 5. Verify the warehouse
```bash
brew install duckdb
duckdb food_delivery.duckdb
```
```sql
SHOW ALL TABLES;
SELECT * FROM main_marts.fct_daily_revenue ORDER BY revenue_date LIMIT 10;
SELECT * FROM main_marts.dim_restaurants WHERE is_top_performer ORDER BY total_revenue_usd DESC;
```

The warehouse exposes three schemas:

- `main_raw` — seeded CSVs
- `main_staging` — type-cast / cleaned views
- `main_marts` — dimensions, facts, and reports (tables / incrementals)

### Useful commands
| Command | Description |
|---|---|
| `dbt debug` | Verify connection to DuckDB, profile, project config |
| `dbt seed` | Load CSVs in `seeds/` into `main_raw` |
| `dbt run` | Build all models (no tests) |
| `dbt test` | Run all tests against existing tables |
| `dbt build` | Seed → run → test in DAG order (one-stop) |
| `dbt run --select staging` | Build only the staging layer |
| `dbt run --select dim_restaurants+` | Build a model and everything downstream |
| `dbt run --full-refresh` | Force-rebuild incremental tables from scratch |
| `dbt compile --select fct_restaurant_daily` | Render a model's SQL into `target/compiled/...` |
| `dbt list --resource-type test` | List every test in the project |
| `dbt docs generate && dbt docs serve` | Build & serve the lineage UI at `localhost:8080` |

---

## Project Architecture

```
food_delivery_dbt/
├── seeds/                                    # raw CSVs (raw layer)
│   ├── raw_customers.csv
│   ├── raw_restaurants.csv
│   ├── raw_menu_items.csv
│   ├── raw_orders.csv
│   ├── raw_order_items.csv
│   └── raw_deliveries.csv
├── models/
│   ├── staging/                              # views, 1:1 with raw + standardize_text
│   │   ├── stg_customers.sql
│   │   ├── stg_restaurants.sql
│   │   ├── stg_menu_items.sql
│   │   ├── stg_orders.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_deliveries.sql
│   │   └── _staging__models.yml
│   └── marts/
│       ├── dim_customers.sql
│       ├── dim_restaurants.sql
│       ├── dim_menu_items.sql
│       ├── dim_couriers.sql                  # surrogate key (md5)
│       ├── fct_orders.sql                    # incremental
│       ├── fct_order_items.sql               # incremental
│       ├── fct_deliveries.sql                # incremental
│       ├── fct_daily_revenue.sql             # incremental + window fn
│       ├── fct_restaurant_daily.sql          # incremental + predicate
│       ├── reports/
│       │   ├── rpt_restaurant_daily_rank.sql
│       │   ├── rpt_courier_avg_delivery_time.sql
│       │   ├── rpt_customer_30_day_avg_spend.sql
│       │   ├── rpt_menu_item_sales_momentum.sql
│       │   ├── rpt_daily_new_customers.sql
│       │   └── _reports__models.yml
│       └── _marts__models.yml
├── macros/
│   └── standardize_text.sql
├── tests/marts/                              # singular tests
│   ├── fct_orders_total_amount_positive.sql
│   ├── fct_order_items_line_total_calculation.sql
│   ├── fct_deliveries_positive_duration_on_delivery.sql
│   └── dim_restaurants_top_performer_logic.sql
├── dbt_project.yml
├── profiles.yml                              # project-local (jaffle-shop pattern)
└── seed_generator.py                         # Faker-based CSV generator
```

### Layered design
- **Raw layer** — `seeds/raw_*.csv`. Materialized as tables in `main_raw` by `dbt seed`. Treated as source-of-truth.
- **Staging layer** — `stg_*` views. One-to-one with raw; renames, type narrowing (DOUBLE → DECIMAL where it matters), and text standardization via the `standardize_text` macro. Cheap to rebuild.
- **Marts layer** — business-facing tables. Split into:
  - **Dimensions** (`dim_*`) — denormalized entity views with attributes and lifetime aggregates.
  - **Facts** (`fct_*`) — event-grain tables with FK to dims; all materialized incrementally.
  - **Reports** (`rpt_*`) — pre-aggregated, window-function-heavy outputs ready for BI.

---

## Implementation Highlights

### Incremental Models (5)

| Model | Grain | `unique_key` | Lookback / filter |
|---|---|---|---|
| `fct_orders` | one row per order | `order_id` | `order_timestamp > max(order_timestamp)` |
| `fct_order_items` | one row per line item | `order_item_id` | new line items only |
| `fct_deliveries` | one row per delivery | `delivery_id` | new deliveries only |
| `fct_daily_revenue` | one row per `revenue_date` | `revenue_date` | recompute last 7 days |
| `fct_restaurant_daily` | one row per `(restaurant_id, revenue_date)` | composite | recompute last 7 days **+ incremental_predicate** |

### Incremental predicate — `fct_restaurant_daily`

```python
config(
    materialized='incremental',
    unique_key=['restaurant_id', 'revenue_date'],
    incremental_strategy='delete+insert',
    incremental_predicates=[
        "DBT_INCREMENTAL_TARGET.revenue_date >= current_date - interval '7 days'"
    ]
)
```

**Why:** with `delete+insert`, dbt deletes target rows whose `unique_key` matches the staging set, then inserts. Without a predicate the DELETE has to scan the **entire historical fact table** to find matches. The predicate tells DuckDB "only the last 7 days can possibly match" — bounding the DELETE scope to the same 7-day window the source query produces.

**Trade-off:** the predicate window must be at least as wide as the source filter (`is_incremental()` clause), or you'll over-produce rows without removing the duplicates being replaced. The lookback also implicitly defines how late "late-arriving" data may be: an order timestamped 10 days ago that arrives today will not be reflected in the daily aggregate, because both filter and predicate ignore it.

### Custom Macro

```sql
-- macros/standardize_text.sql
{% macro standardize_text(column_name) %}
    nullif(trim(lower({{ column_name }})), '')
{% endmacro %}
```

Used across every staging model on text columns. It does three things in one pass:
1. `lower()` — normalize case (`'Pizza'` and `'pizza'` group together).
2. `trim()` — strip stray whitespace from CSV input.
3. `nullif(..., '')` — collapse empty strings to true SQL `NULL` so `not_null` tests behave correctly.

**Why a macro and not copied SQL:** the rule is centralized. If we later decide `standardize_text` should also strip punctuation, we change one file instead of touching every staging model. It also makes intent clearer than the nested function call.

### Window Functions (5 models)

| Model | Function | Purpose |
|---|---|---|
| `fct_daily_revenue` | `sum() over (order by order_date)` | Running revenue total |
| `rpt_restaurant_daily_rank` | `rank() over (partition by revenue_date)` | Daily restaurant leaderboard |
| `rpt_customer_30_day_avg_spend` | `avg() over (... rows between 29 preceding and current row)` | Rolling 30-day average per customer |
| `rpt_menu_item_sales_momentum` | `lag()` | Day-over-day growth rate per menu item |
| `rpt_daily_new_customers` | `sum() over (order by signup_date)` | Cumulative customer base |

### Tests

**Generic (in YAML):** `unique`, `not_null`, `relationships`, `accepted_values` — across staging and marts. See `_staging__models.yml`, `_marts__models.yml`, `_reports__models.yml`.

**Singular (`tests/marts/`):**

| Test | What it checks |
|---|---|
| `fct_orders_total_amount_positive` | No order has a negative `total_amount_usd` |
| `fct_order_items_line_total_calculation` | `line_total_usd = quantity × unit_price_usd` (arithmetic integrity) |
| `fct_deliveries_positive_duration_on_delivery` | Deliveries marked `'delivered'` have a positive duration |
| `dim_restaurants_top_performer_logic` | Recomputes `is_top_performer` from `stg_orders` and compares to the dimension flag (business-rule integrity) |

---

## Business Insights

All queries read from the marts layer, never from `seeds/` directly.

### 1. Top restaurants by revenue (current snapshot)
```sql
select
    restaurant_id, restaurant_name, cuisine_type, total_revenue_usd
from main_marts.dim_restaurants
where is_top_performer
order by total_revenue_usd desc
limit 10;
```
Identifies merchants that pass the **$2,500 lifetime delivered-revenue threshold** — candidates for co-marketing.

### 2. Daily restaurant leaderboard
```sql
select revenue_date, restaurant_id, daily_revenue_usd
from main_marts.rpt_restaurant_daily_rank
where daily_revenue_rank = 1
order by revenue_date desc;
```
`rank() over (partition by revenue_date order by daily_revenue_usd desc)` surfaces the **#1 restaurant per day** — distinguishes one-off promotional spikes from consistent winners.

### 3. Courier performance vs. platform average
```sql
select courier_id, avg_delivery_duration_min, diff_from_overall_avg_min
from main_marts.rpt_courier_avg_delivery_time
order by diff_from_overall_avg_min;
```
Negative `diff_from_overall_avg_min` = faster than average. Drives **bonus eligibility** and **retraining lists**.

### 4. Customer rolling spend (30-day momentum)
```sql
select customer_id, order_date, rolling_30_day_avg_spend_usd
from main_marts.rpt_customer_30_day_avg_spend
qualify row_number() over (partition by customer_id order by order_date desc) = 1;
```
Latest rolling average per customer — feeds **VIP segmentation** and **churn-risk alerts**.

### 5. Menu item sales momentum
```sql
select menu_item_id, order_date, daily_sales_growth_rate
from main_marts.rpt_menu_item_sales_momentum
where daily_sales_growth_rate > 0.5
order by order_date desc;
```
Surfaces dishes whose daily revenue **grew >50% day-over-day** — homepage-feature candidates.

### 6. Customer acquisition curve
```sql
select signup_date, new_customers, running_total_customers
from main_marts.rpt_daily_new_customers
order by signup_date;
```
Daily new-customer count + cumulative platform size. Tracks **organic growth vs. campaign spikes**.

### 7. Revenue trend with running total
```sql
select order_date, daily_revenue_usd, running_total_revenue_usd
from main_marts.fct_daily_revenue
order by order_date;
```
Both the daily figure and the cumulative line — the cumulative curve makes growth deceleration immediately visible.

---

### Theoretical Questions about dbt

1. What is the purpose of dbt in a modern data stack?
   - dbt is the **transformation** layer of ELT: data is loaded raw into the warehouse first, and dbt then turns SQL `SELECT` statements into managed, tested, version-controlled tables and views. It brings software-engineering practices (modularity, dependency management, tests, documentation, CI) to analytics SQL, so the same artifacts that produce dashboards are reproducible, observable, and reviewable.

2. What is the difference between a seed, a source, and a model in dbt?
   - **Seed** — a small CSV file dbt loads into the warehouse as a table via `dbt seed`. Owned by the project; good for static lookups and reference data. Used here for the synthetic raw layer.
   - **Source** — a *declaration* of an external table that already exists in the warehouse (loaded by some other tool). Configured in YAML, referenced via `{{ source('schema','table') }}`. Lets dbt test and document data it does not own.
   - **Model** — a `.sql` file containing one `SELECT`. dbt materializes it as a view, table, incremental table, or ephemeral CTE. The output of one model can be referenced from another via `{{ ref('model_name') }}`, which is how dbt builds the DAG.

3. What is the difference between table, view, and incremental materializations?
   - **View** — dbt creates a database view; the SQL re-runs every time the view is queried. Cheap to build, slow to query, always fresh. Default for staging in this project.
   - **Table** — dbt creates a table by running `CREATE TABLE AS SELECT`. Full rebuild on every `dbt run`. Fast to query, expensive to refresh on large data. Default for marts.
   - **Incremental** — dbt creates the table on first run, and on subsequent runs only inserts/updates rows matching the `is_incremental()` filter and (optionally) `unique_key`. Best for append-mostly large fact tables. Adds operational complexity (lookback windows, late-arriving data) in exchange for cheap refreshes.

4. What is the purpose of the staging layer in a dbt project?
   - The staging layer is a **thin, one-to-one cleanup** between raw source tables and downstream business logic. Its job is to rename columns to the project's convention, cast types correctly, apply trivial cleanups (whitespace, casing, empty strings via `standardize_text`), and stop there — no joins, no aggregates, no business rules. This isolates the rest of the project from raw schema drift: when the source adds or renames a column, only one staging model changes.

5. What is the difference between a dimension model and a fact model?
   - **Dimension (`dim_*`)** — describes *who/what/where*. Entities and their attributes (customers, restaurants, menu items). One row per entity. Tend to be small, slowly changing, and reused as join targets.
   - **Fact (`fct_*`)** — records *what happened*. Events and measurements (orders, order items, deliveries) at a defined grain. Foreign keys point at the dimensions. Tend to be large, append-mostly, and the natural home for incremental materialization. In this project every `fct_*` references one or more `dim_*` via foreign keys validated by `relationships` tests.

6. Why are tests important in dbt, and what is the difference between generic and singular tests?
   - Tests turn invariants into executable assertions, run on every build, that catch upstream schema drift, broken joins, and silent business-logic bugs before they reach a dashboard.
   - **Generic tests** are reusable and configured declaratively in YAML (`unique`, `not_null`, `relationships`, `accepted_values`). One YAML block tests many columns.
   - **Singular tests** are bespoke `SELECT` statements stored under `tests/`. They pass when they return zero rows. Used here to encode rules that don't fit a generic shape — e.g., "for every delivered delivery, duration must be positive" or "the `is_top_performer` flag must agree with a recomputation from `stg_orders`".

7. What is a macro in dbt, and when should you create one?
   - A macro is a Jinja function — a piece of templated SQL that can take arguments and be reused across models, tests, and configs. Create one when you find yourself **copy-pasting non-trivial SQL** into multiple models, when a transformation rule needs a single source of truth, or when you want to abstract over column lists. Don't create one for a one-off snippet; macros add indirection. The `standardize_text` macro in this project is justified because the same text-normalization rule is applied across six staging models.

8. What is an incremental predicate, and how does it improve model performance?
   - An incremental predicate is an extra `WHERE` clause that dbt injects into the **destination side** of an incremental write — most visibly into the `DELETE` of a `delete+insert` strategy, or as a partition-pruning hint to a `MERGE`. Without a predicate the warehouse must consider the **entire historical target table** when looking for rows to replace, even when we know the matches must be in a narrow recent window. The predicate tells the planner "only this slice can possibly match," which lets it prune partitions / skip files / avoid full scans. In this project, `fct_restaurant_daily` uses `revenue_date >= current_date - interval '7 days'` so only the last week of the destination is touched.

9. Why are window functions useful in analytics engineering?
   - Window functions perform calculations **across a set of rows related to the current row** without collapsing the result set the way `GROUP BY` does. That makes them the natural tool for the questions analytics engineers are asked most: ranking (`rank`, `row_number`), comparisons over time (`lag`, `lead`), running totals and rolling averages (`sum() over`, `avg() over`), and partition-scoped aggregates kept alongside detail rows. They let a single CTE answer "what is the current row's value **and** how does it compare to its peers / yesterday / its 30-day average?" — exactly the framing most BI questions take.

10. What does it mean to describe the grain of a model, and why is grain important?
    - The **grain** is the precise definition of what a single row in the model represents. For example: `fct_orders` — one row per order; `fct_order_items` — one row per line item within an order; `fct_restaurant_daily` — one row per `(restaurant_id, revenue_date)` pair. Grain matters because **every aggregation, every join, and every uniqueness test depends on it**. Joining two facts at different grains without thinking double-counts. Forgetting the grain when writing a `SUM` produces a number with no clear definition. Stating the grain in the model description (and enforcing it with a `unique` test on the natural or composite key) makes the model self-documenting and safe to compose.
