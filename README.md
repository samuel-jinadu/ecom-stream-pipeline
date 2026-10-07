# Ecom Pipeline: Real-Time E-commerce Analytics with Kafka & Postgres

A streaming data pipeline that ingests e-commerce orders, products, and inventory events, enriches them in near real-time, detects low-stock situations, and surfaces live business metrics through a Streamlit dashboard.

---

## Overview

This project simulates an e-commerce platform’s event stream and demonstrates an end-to-end data engineering workflow:

- **Generate** synthetic products, inventory, and orders as CSV files.
- **Produce** them as JSON events into Kafka topics.
- **Consume** and process those streams with Python workers.
- **Enrich** orders with product prices to compute revenue.
- **Alert** on low inventory.
- **Store** results in PostgreSQL.
- **Visualize** top products, low stock, and alerts on a live dashboard.

It runs either fully in Docker Compose or locally with `uv`, Kafka, and Postgres.

---

## The Data Engineering Problem

Traditional batch reporting answers questions like *“What were yesterday’s top products?”* only after a full ETL cycle. That creates several problems:

- **Stale dashboards** – business users see data that is hours or days old.
- **Delayed alerts** – low-stock situations are discovered too late.
- **Tight coupling** – reporting logic reads directly from source tables, making it hard to add new consumers.
- **Reprocessing overhead** – every new metric often requires rerunning the entire batch job.

In short: the business needs **fresh, continuously updated metrics and alerts**, not a nightly snapshot.

---

## How It Solves It

The pipeline replaces the batch-only mindset with a **streaming, event-driven architecture**:

1. **Decoupled ingestion** – Orders, products, and inventory are published as independent Kafka topics. Producers and consumers scale and evolve separately.
2. **Stream–table join for enrichment** – The aggregator first consumes the `products` topic into an in-memory lookup table, then joins each `orders` event with its product price to compute `revenue` in real time.
3. **Continuous alerting** – The inventory consumer evaluates every stock update and emits a `LOW_STOCK` alert to both Kafka and Postgres when stock drops below 5.
4. **Materialized views for fast reads** – PostgreSQL views (`top_ten_products`, `low_stock_products`) pre-define business metrics so the dashboard queries stay simple and fast.
5. **Idempotent, at-least-once processing** – Producers use idempotent writes with `acks=all`; consumers commit offsets manually after writing; inserts use `ON CONFLICT DO NOTHING`. This makes replays safe and prevents duplicates.
6. **Live dashboard** – Streamlit auto-refreshes every 2 seconds, reading directly from Postgres views, so the UI always reflects the latest committed state.

The result is a pipeline where yesterday’s revenue, current low-stock items, and recent alerts are available **while events are still flowing**.

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  CSV Generator  │────▶│    Producer     │────▶│     Kafka       │
│ (Faker + Pandas)│     │ (3 topics)      │     │ orders/products │
└─────────────────┘     └─────────────────┘     │ /inventory      │
                                                 └────────┬────────┘
                                                          │
                     ┌────────────────────────────────────┼────────────────────────────────────┐
                     │                                    │                                    │
                     ▼                                    ▼                                    ▼
          ┌─────────────────────┐            ┌─────────────────────┐            ┌─────────────────────┐
          │  Orders Consumer    │            │  Join Orders +      │            │  Inventory Alert    │
          │  (write raw orders) │            │  Products Consumer  │            │  Processor          │
          └─────────────────────┘            │  (enrich revenue)   │            │  (stock < 5)        │
                                             └──────────┬──────────┘            └──────────┬──────────┘
                                                        │                                  │
                                                        ▼                                  ▼
                                             ┌─────────────────────────────────────────────────────┐
                                             │                  PostgreSQL                         │
                                             │  orders, orders_enriched, inventory, alerts,        │
                                             │  products, top_ten_products, low_stock_products     │
                                             └──────────────────────────┬──────────────────────────┘
                                                                        │
                                                                        ▼
                                                             ┌─────────────────────┐
                                                             │  Streamlit Dashboard│
                                                             │  (auto-refresh 2s)  │
                                                             └─────────────────────┘
```

---

## Data Flow

1. `ecom-pipeline` generates `products.csv`, `inventory.csv`, and `orders.csv` under `data/generated/`.
2. `producer` reads those CSVs and publishes records to Kafka topics:
   - `orders` (keyed by `order_id`)
   - `products` (keyed by `id`, compacted topic)
   - `inventory` (keyed by `product_id`)
3. `aggregator` runs multiple consumers in parallel:
   - Writes raw `orders`, `products`, `inventory` rows to Postgres.
   - Builds an in-memory product lookup from the `products` topic, then enriches each `orders` event with `revenue = price × quantity` and writes to `orders_enriched`.
   - Watches `inventory` for stock < 5 and produces `alerts` to Kafka and Postgres.
4. The dashboard reads `top_ten_products`, `low_stock_products`, and `alerts` views every 2 seconds.

---

## Components

| Component | Entry point | Description |
|-----------|-------------|-------------|
| **Generator** | `uv run ecom-pipeline` | Creates synthetic CSVs and a batch report. |
| **Producer** | `uv run producer` | Publishes CSV rows to Kafka topics. |
| **Aggregator** | `uv run aggregator` | Consumes streams, enriches orders, writes to Postgres, emits alerts. |
| **Alerts CLI** | `uv run alerts` | Runs only the alert-producing consumer. |
| **Dashboard** | `uv run dashboard` | Launches Streamlit UI. |
| **Batch report** | `ecom_pipeline.batch_report` | Computes top-ten products from CSVs and saves CSV/SQLite. |

---

## Data Model

Defined in `data/schema.sql`:

- `orders` – raw order events.
- `orders_enriched` – orders joined with product price and computed revenue.
- `inventory` – current stock per product.
- `products` – product catalog.
- `alerts` – low-stock alert records.
- `top_ten_products` (view) – top 10 products by revenue for yesterday.
- `low_stock_products` (view) – products with stock < 10.

---

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose **or**
- Python 3.12+, [`uv`](https://docs.astral.sh/uv/), a local Kafka installation, and PostgreSQL 16.

### Option 1: Docker Compose (recommended)

```bash
docker compose up --build
```

Then open the dashboard at [http://localhost:8501](http://localhost:8501).

This starts:
- PostgreSQL 16 with the schema auto-loaded.
- Apache Kafka (KRaft mode, no ZooKeeper).
- `app-producer` – generates CSVs and streams them.
- `app-aggregator` – consumes streams and writes to Postgres.
- `dashboard` – Streamlit UI.

To stop and remove volumes:

```bash
docker compose down -v
```

### Option 2: Local Development

1. **Install dependencies**

   ```bash
   uv sync
   ```

2. **Set up environment variables**

   Create `.env` in the project root:

   ```env
   KAFKA_BOOTSTRAP=localhost:9092
   POSTGRES_DSN=postgresql://kafka_user:1234@localhost:5432/ecommerce
   CREATE_TABLE_SQL_PATH=./data/schema.sql
   ```

   Adjust the DSN to match your local Postgres user/password.

3. **Set up PostgreSQL**

   Create the database `ecommerce` and apply `data/schema.sql`:

   ```bash
   psql -U kafka_user -h localhost -d ecommerce -f data/schema.sql
   ```

4. **Install and start Kafka**

   ```bash
   bash scripts/install-kafka.sh
   bash scripts/run-kafka.sh
   ```

   The script formats storage, starts the broker, and tails logs. Use `bash scripts/stop-kafka.sh` to stop it.

5. **Generate data and run the batch report**

   ```bash
   uv run ecom-pipeline
   ```

6. **Start the producer** (in one terminal)

   ```bash
   uv run producer
   ```

7. **Start the aggregator** (in another terminal)

   ```bash
   uv run aggregator
   ```

8. **Start the dashboard** (in a third terminal)

   ```bash
   uv run dashboard
   ```

   Visit [http://localhost:8501](http://localhost:8501).

---

## Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP` | Kafka bootstrap server(s). | `localhost:9092` or `kafka:9092` |
| `POSTGRES_DSN` | PostgreSQL connection string. | `postgresql://postgres:postgres@localhost:5432/ecommerce` |
| `CREATE_TABLE_SQL_PATH` | Path to the schema SQL file. | `./data/schema.sql` |

For Docker Compose, these are set automatically in `compose.yaml`. For local runs, use `.env`.

---

## Usage

- **Reset Kafka state** (local only):

  ```bash
  bash scripts/reset-kafka.sh
  ```

- **Check Kafka processes**:

  ```bash
  bash scripts/check-kafka.sh
  ```

- **Check Postgres row counts**:

  ```bash
  bash scripts/check-postgres.sh
  ```

- **Run only the alert stream**:

  ```bash
  uv run alerts
  ```

---

## Project Structure

```
.
├── compose.yaml                 # Docker Compose stack
├── Dockerfile.app               # App image (Python + uv)
├── Dockerfile.deprecated        # Old local Kafka image
├── pyproject.toml               # Dependencies and entry points
├── .env                         # Local environment variables
├── data/
│   ├── schema.sql               # Postgres schema + views
│   ├── generated/               # Generated CSVs
│   └── processed/               # Batch report output
├── ecom_pipeline/
│   ├── __main__.py              # `ecom-pipeline` entry point
│   ├── generate.py              # Faker-based CSV generation
│   ├── producer.py              # Kafka producers
│   ├── aggregator.py            # Kafka consumers + Postgres writes
│   ├── postgres.py              # Connection pool + helpers
│   ├── dashboard.py             # Streamlit UI
│   ├── batch_report.py          # Batch top-ten report
│   └── stream_report.py         # Placeholder
├── scripts/
│   ├── install-kafka.sh         # Download and extract Kafka
│   ├── run-kafka.sh             # Start Kafka locally
│   ├── stop-kafka.sh            # Stop Kafka
│   ├── reset-kafka.sh           # Remove Kafka data
│   ├── check-kafka.sh           # List Kafka processes
│   ├── check-postgres.sh        # Row counts
│   └── kafka-common.sh          # Shared shell helpers
└── kafka/
    ├── server.properties.template
    └── server.properties        # Rendered config
```

---

## Why This Matters

This project is a compact example of the **streaming lakehouse pattern**:

- Events are first-class citizens.
- Consumers are independent and replayable.
- Enrichment happens in-flight, not in a nightly batch.
- Storage and serving layers are separated.
- The dashboard is a thin read layer over materialized views.

It shows how to move from “report yesterday’s numbers tomorrow” to “see what’s happening now.”

---

## License

MIT License. See [LICENSE](./LICENSE) for details.