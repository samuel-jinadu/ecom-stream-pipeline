import os
from pathlib import Path

from dotenv import load_dotenv
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from tenacity import retry, stop_after_attempt

load_dotenv()

_required = ["CREATE_TABLE_SQL_PATH", "POSTGRES_DSN"]
_missing = [k for k in _required if not os.getenv(k)]
if _missing:
    raise RuntimeError(f"Missing env var(s): {', '.join(_missing)}")

POSTGRES_DSN = os.getenv("POSTGRES_DSN")
CREATE_TABLE_SQL_PATH = os.getenv("CREATE_TABLE_SQL_PATH")


pool = ConnectionPool(
    POSTGRES_DSN, min_size=2, max_size=8, kwargs={"row_factory": dict_row}
)




def get_schema_sql():
    path = Path(CREATE_TABLE_SQL_PATH)
    if not path.exists():
        raise RuntimeError(
            "schema for creating the table the aggregator writes to not found!"
        )
    with open(path, "r") as f:
        return f.read()


def make_tables():
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(get_schema_sql())
        conn.commit()


@retry(stop=stop_after_attempt(3))
def write_data_to_postgres(row: dict, table: str):
    cols = sql.SQL(", ").join(sql.Identifier(k) for k in row)
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in row)
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
        sql.Identifier(table), cols, placeholders
    )
    with pool.connection() as conn, conn.cursor() as cur:
        # print(f"Writing '{query}' to postgres")
        cur.execute(query, list(row.values()))
        conn.commit()


def get_data_from_postgres(table):
    with pool.connection() as conn, conn.cursor() as cur:
        return cur.execute(
            sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))
        ).fetchall()
