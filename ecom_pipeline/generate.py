from pathlib import Path

import pandas as pd
from faker import Faker
from faker_ecommerce import EcommerceProvider

NO_OF_PRODUCTS = 100
NO_OF_ORDERS = 5000
fake = Faker()
fake.add_provider(EcommerceProvider)
Faker.seed(0)


def generate_products() -> pd.DataFrame:
    products_df = pd.DataFrame(columns=["id", "name", "price"])
    for i in range(NO_OF_PRODUCTS):
        new_row = pd.DataFrame(
            [{"id": fake.sku(), "name": fake.product_name(), "price": fake.price()}]
        )
        products_df = pd.concat([products_df, new_row])
    return products_df


def generate_inventory(products_df: pd.DataFrame) -> pd.DataFrame:
    products = products_df["id"].to_list()
    inventory_df = pd.DataFrame(columns=["product_id", "stock"])
    for product in products:
        new_row = pd.DataFrame(
            [{"product_id": product, "stock": fake.random_int(min=1, max=99)}]
        )
        inventory_df = pd.concat([inventory_df, new_row])
    return inventory_df


def generate_orders(products_df: pd.DataFrame) -> pd.DataFrame:
    products = products_df["id"].to_list()
    orders_df = pd.DataFrame(
        columns=["order_id", "order_date", "product_id", "quantity"]
    )
    for i in range(NO_OF_ORDERS):
        new_row = pd.DataFrame(
            [
                {
                    "order_id": fake.order_id(),
                    "order_date": fake.date_between(start_date="-1y", end_date="today"),
                    "product_id": fake.random_choices(elements=products, length=1)[0],
                    "quantity": fake.random_int(min=1, max=9),
                }
            ]
        )
        orders_df = pd.concat([orders_df, new_row])
    return orders_df


def generate_csv(path: Path):
    products_df = generate_products()
    inventory_df = generate_inventory(products_df)
    orders_df = generate_orders(products_df)

    products_df.to_csv(path / "products.csv", index=False)
    inventory_df.to_csv(path / "inventory.csv", index=False)
    orders_df.to_csv(path / "orders.csv", index=False)

    return 0
