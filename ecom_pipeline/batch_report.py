import sqlite3
from pathlib import Path

import pandas as pd


def get_top_ten_products(product_csv_path: Path, orders_csv_path: Path):
    products_df = pd.read_csv(product_csv_path)
    orders_df = pd.read_csv(orders_csv_path)

    orders_df["order_date"] = pd.to_datetime(orders_df["order_date"])
    one_day_ago = (pd.Timestamp.now() - pd.Timedelta(days=1)).normalize()
    recent_orders_df = orders_df[orders_df["order_date"] == one_day_ago]

    df = recent_orders_df.merge(
        products_df, left_on="product_id", right_on="id", how="inner"
    )

    df["price"] = df["price"].apply(lambda x: float(x.lstrip("$")))
    df["revenue"] = df["price"] * df["quantity"]

    top_ten_df = (
        df[["id", "name", "revenue"]]
        .groupby(["id", "name"], as_index=False)
        .sum()
        .sort_values("revenue", ascending=False)
        .head(10)
    )

    return top_ten_df


def save_report(df: pd.DataFrame, path: Path):
    print(df)
    df.to_csv(path / "report.csv")
    with sqlite3.connect(path / "report.db") as conn:
        df.to_sql("report", conn, if_exists="replace", index=False)
