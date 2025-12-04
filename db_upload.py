from dotenv import load_dotenv
import os
from mysql.connector import connect, Error
import csv

load_dotenv()


def connect_db():
    try:
        user = os.getenv("SQL_USER")
        password = os.getenv("SQL_PASSWORD")
        database = os.getenv("DATABASE_NAME")
        connection = connect(
            user=user, password=password, host="localhost", database=database
        )
        return connection
    except Error as e:
        print("Error connecting to database\nError:", e)
        return None


def create_tables():
    conn = connect_db()
    if conn is None:
        return

    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS cryptocurrencies (
        id INT PRIMARY KEY auto_increment,
        name VARCHAR(100),
        symbol VARCHAR(10),
        market_cap BIGINT,
        price DECIMAL(20, 2),
        volume_24h BIGINT,
        percent_change_1h DECIMAL(10, 2),
        percent_change_24h DECIMAL(10, 2),
        percent_change_7d DECIMAL(10, 2),
        last_updated DATETIME
    );
    """
    try:
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'cryptocurrencies' created successfully.")
    except Error as e:
        print("Error creating table\nError:", e)
    finally:
        cursor.close()
        conn.close()


def add_data():
    try:
        conn = connect_db()
        if conn is None:
            return

        cursor = conn.cursor()
        insert_query = """
        INSERT INTO cryptocurrencies (
            name, symbol, price, market_cap, volume_24h,
            percent_change_1h, percent_change_24h, percent_change_7d,
            last_updated
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name=VALUES(name),
            price=VALUES(price),
            market_cap=VALUES(market_cap),
            volume_24h=VALUES(volume_24h),
            percent_change_1h=VALUES(percent_change_1h),
            percent_change_24h=VALUES(percent_change_24h),
            percent_change_7d=VALUES(percent_change_7d),
            last_updated=VALUES(last_updated);
        """

        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(BASE_DIR, "logs", "cleaned.csv")

        with open(file_path, "r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            # Ensure the required columns exist
            required_cols = [
                "name",
                "symbol",
                "price_usd",
                "market_cap_usd",
                "volume_24h_usd",
                "percent_change_1h_usd",
                "percent_change_24h_usd",
                "percent_change_7d_usd",
                "acquired_timestamp",
            ]
            for col in required_cols:
                if col not in reader.fieldnames:
                    raise KeyError(f"Column '{col}' not found in CSV!")

            rows = []
            for row in reader:
                rows.append(
                    (
                        row["name"],
                        row["symbol"],
                        round(float(row["price_usd"]), 2),
                        round(float(row["market_cap_usd"]), 2),
                        round(float(row["volume_24h_usd"]), 2),
                        round(float(row["percent_change_1h_usd"]), 5),
                        round(float(row["percent_change_24h_usd"]), 5),
                        round(float(row["percent_change_7d_usd"]), 5),
                        row["acquired_timestamp"],
                    )
                )

            if rows:
                cursor.executemany(insert_query, rows)
                conn.commit()
                print("Data inserted/updated successfully.")
            else:
                print("No data found in CSV to insert.")

    except KeyError as ke:
        print(f"Error: {ke}")
    except Error as e:
        print("Error inserting/updating data\nError:", e)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    create_tables()
    add_data()
