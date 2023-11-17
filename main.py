import os
import csv
import psycopg2


def execute_sql_script(cursor, script):
    cursor.execute(script)

def create_accounts_table(cursor):
    script = """
    CREATE TABLE IF NOT EXISTS accounts (
        customer_id INT PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        address_1 VARCHAR,
        address_2 VARCHAR,
        city VARCHAR,
        state VARCHAR,
        zip_code VARCHAR,
        join_date DATE
    );
    """
    execute_sql_script(cursor, script)

def create_products_table(cursor):
    script = """
    CREATE TABLE IF NOT EXISTS products (
        product_id INT PRIMARY KEY,
        product_code VARCHAR UNIQUE,
        product_description VARCHAR
    );
    """
    execute_sql_script(cursor, script)

def create_transactions_table(cursor):
    script = """
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id VARCHAR PRIMARY KEY,
        transaction_date DATE,
        product_id INT,
        product_code VARCHAR,
        product_description VARCHAR,
        quantity INT,
        account_id INT,
        FOREIGN KEY (product_id) REFERENCES products(product_id),
        FOREIGN KEY (account_id) REFERENCES accounts(customer_id)
    );
    """
    execute_sql_script(cursor, script)

def insert_data_from_csv(cursor, table_name, csv_filename):
    with open(csv_filename, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        header = next(reader)

        query = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES ({', '.join(['%s'] * len(header))})"

        for row in reader:
            cursor.execute(query, tuple(row))

def main():
    data_folder = 'data'
    host = "localhost"
    database = "lab4data"
    user = "ahostiuk"
    pas = "Pa55word"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)

    try:
        cursor = conn.cursor()

        create_accounts_table(cursor)
        create_products_table(cursor)
        create_transactions_table(cursor)

        insert_data_from_csv(cursor, 'accounts', os.path.join(data_folder, 'accounts.csv'))
        insert_data_from_csv(cursor, 'products', os.path.join(data_folder, 'products.csv'))
        insert_data_from_csv(cursor, 'transactions', os.path.join(data_folder, 'transactions.csv'))

        conn.commit()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
