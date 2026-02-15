import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_FILES_DIR = os.path.join(BASE_DIR, "raw_files")


# =============================
# Database Connection
# =============================

def get_connection():
    return psycopg2.connect(
        dbname="olist_dw",
        user="vishalbondala",
        host="localhost",
        port="5432"
    )


# =============================
# Watermark Functions
# =============================

def get_next_business_date(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT business_date, end_date
            FROM pipeline_progress
            WHERE id = 1;
        """)
        row = cur.fetchone()

    if not row:
        raise Exception("pipeline_progress table not initialized.")

    business_date, end_date = row

    if business_date > end_date:
        print("All historical dates processed. Exiting.")
        return None

    return business_date


def update_business_date(conn, next_date):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_progress
            SET business_date = %s
            WHERE id = 1;
        """, (next_date,))
    conn.commit()


# =============================
# Orders Loader
# =============================

def load_orders(conn, execution_date):
    print("Loading orders...")

    orders_path = os.path.join(RAW_FILES_DIR, "olist_orders_dataset.csv")
    df = pd.read_csv(
        orders_path,
        parse_dates=[
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]
    )

    df = df[
        df["order_purchase_timestamp"].dt.date == execution_date
    ]

    if df.empty:
        print("No orders for this date.")
        return []

    df = df.where(pd.notnull(df), None)

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO raw.orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (order_id) DO NOTHING
            """, tuple(None if pd.isna(v) else v for v in row.values))

    conn.commit()

    print(f"Inserted {len(df)} orders.")
    return df["order_id"].tolist()


# =============================
# Generic Child Loader
# =============================

def load_child_table(conn, file_name, table_name, key_column, valid_keys):

    if not valid_keys:
        return

    print(f"Loading {table_name}...")

    file_path = os.path.join(RAW_FILES_DIR, file_name)
    df = pd.read_csv(file_path)

    df = df[df[key_column].isin(valid_keys)]

    if df.empty:
        print(f"No records for {table_name}.")
        return

    df = df.where(pd.notnull(df), None)

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            placeholders = ",".join(["%s"] * len(row))
            cur.execute(
                f"INSERT INTO raw.{table_name} VALUES ({placeholders})",
                tuple(None if pd.isna(v) else v for v in row.values)
            )

    conn.commit()
    print(f"Inserted {len(df)} rows into {table_name}.")


# =============================
# Reviews Loader
# =============================

def load_reviews(conn, execution_date):
    print("Loading reviews...")

    reviews_path = os.path.join(RAW_FILES_DIR, "olist_order_reviews_dataset.csv")
    df = pd.read_csv(
        reviews_path,
        parse_dates=["review_creation_date", "review_answer_timestamp"]
    )

    df = df[df["review_creation_date"].dt.date == execution_date]

    if df.empty:
        print("No reviews for this date.")
        return

    with conn.cursor() as cur:
        cur.execute("SELECT order_id FROM raw.orders;")
        existing_orders = set(r[0] for r in cur.fetchall())

    df = df[df["order_id"].isin(existing_orders)]

    if df.empty:
        print("No valid reviews (parent order missing).")
        return

    df = df.where(pd.notnull(df), None)

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO raw.reviews VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, tuple(None if pd.isna(v) else v for v in row.values))

    conn.commit()
    print(f"Inserted {len(df)} reviews.")


# =============================
# Dimension Loader
# =============================

def load_dimensions(conn, order_ids):

    if not order_ids:
        return

    print("Loading dimensions...")

    # Customers
    orders_path = os.path.join(RAW_FILES_DIR, "olist_orders_dataset.csv")
    orders_df = pd.read_csv(orders_path)

    orders_df = orders_df[orders_df["order_id"].isin(order_ids)]
    customer_ids = orders_df["customer_id"].unique()

    customers_path = os.path.join(RAW_FILES_DIR, "olist_customers_dataset.csv")
    customers_df = pd.read_csv(customers_path)

    customers_df = customers_df[
        customers_df["customer_id"].isin(customer_ids)
    ]

    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM raw.customers;")
        existing_customers = set(r[0] for r in cur.fetchall())

    customers_df = customers_df[
        ~customers_df["customer_id"].isin(existing_customers)
    ]

    customers_df = customers_df.where(pd.notnull(customers_df), None)

    if not customers_df.empty:
        with conn.cursor() as cur:
            for _, row in customers_df.iterrows():
                cur.execute("""
                    INSERT INTO raw.customers VALUES (%s,%s,%s,%s,%s)
                """, tuple(None if pd.isna(v) else v for v in row.values))
        conn.commit()
        print(f"Inserted {len(customers_df)} customers.")

    # Products & Sellers
    items_path = os.path.join(RAW_FILES_DIR, "olist_order_items_dataset.csv")
    items_df = pd.read_csv(items_path)
    items_df = items_df[items_df["order_id"].isin(order_ids)]

    product_ids = items_df["product_id"].unique()
    seller_ids = items_df["seller_id"].unique()

    # Products
    products_path = os.path.join(RAW_FILES_DIR, "olist_products_dataset.csv")
    products_df = pd.read_csv(products_path)
    products_df = products_df[
        products_df["product_id"].isin(product_ids)
    ]

    with conn.cursor() as cur:
        cur.execute("SELECT product_id FROM raw.products;")
        existing_products = set(r[0] for r in cur.fetchall())

    products_df = products_df[
        ~products_df["product_id"].isin(existing_products)
    ]

    products_df = products_df.where(pd.notnull(products_df), None)

    if not products_df.empty:
        with conn.cursor() as cur:
            for _, row in products_df.iterrows():
                cur.execute("""
                    INSERT INTO raw.products VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, tuple(None if pd.isna(v) else v for v in row.values))
        conn.commit()
        print(f"Inserted {len(products_df)} products.")

    # Sellers
    sellers_path = os.path.join(RAW_FILES_DIR, "olist_sellers_dataset.csv")
    sellers_df = pd.read_csv(sellers_path)
    sellers_df = sellers_df[
        sellers_df["seller_id"].isin(seller_ids)
    ]

    with conn.cursor() as cur:
        cur.execute("SELECT seller_id FROM raw.sellers;")
        existing_sellers = set(r[0] for r in cur.fetchall())

    sellers_df = sellers_df[
        ~sellers_df["seller_id"].isin(existing_sellers)
    ]

    sellers_df = sellers_df.where(pd.notnull(sellers_df), None)

    if not sellers_df.empty:
        with conn.cursor() as cur:
            for _, row in sellers_df.iterrows():
                cur.execute("""
                    INSERT INTO raw.sellers VALUES (%s,%s,%s,%s)
                """, tuple(None if pd.isna(v) else v for v in row.values))
        conn.commit()
        print(f"Inserted {len(sellers_df)} sellers.")


# =============================
# Main
# =============================

def main():

    conn = get_connection()

    business_date = get_next_business_date(conn)

    if business_date is None:
        conn.close()
        return

    print(f"\nProcessing business date: {business_date}\n")

    order_ids = load_orders(conn, business_date)

    load_child_table(
        conn,
        "olist_order_items_dataset.csv",
        "order_items",
        "order_id",
        order_ids
    )

    load_child_table(
        conn,
        "olist_order_payments_dataset.csv",
        "payments",
        "order_id",
        order_ids
    )

    load_dimensions(conn, order_ids)
    load_reviews(conn, business_date)

    next_date = business_date + timedelta(days=1)
    update_business_date(conn, next_date)

    conn.close()
    print("\nIngestion completed.\n")


if __name__ == "__main__":
    main()