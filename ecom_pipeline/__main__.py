import sys
from pathlib import Path

from ecom_pipeline.batch_report import get_top_ten_products, save_report
from ecom_pipeline.generate import generate_csv


def main():
    generate_csv(Path("./data/generated"))
    df = get_top_ten_products(
        Path("./data/generated") / "products.csv",
        Path("./data/generated") / "orders.csv",
    )
    save_report(df, Path("./data/processed"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
