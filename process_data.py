import os
import pandas as pd


def load_raw(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print("Error loading raw data\nError:", e)
        return None


def convert_to_required_types(df):
    try:
        df["name"] = df["name"].astype(str)
        df["symbol"] = df["symbol"].astype(str)
        df["price_usd"] = df["price_usd"].astype(float)
        df["market_cap_usd"] = df["market_cap_usd"].astype(float)
        df["volume_24h_usd"] = df["volume_24h_usd"].astype(float)
        df["percent_change_1h_usd"] = df["percent_change_1h_usd"].astype(float)
        df["percent_change_24h_usd"] = df["percent_change_24h_usd"].astype(float)
        df["percent_change_7d_usd"] = df["percent_change_7d_usd"].astype(float)
        df["acquired_timestamp"] = pd.to_datetime(df["acquired_timestamp"])

        df["price_usd"] = df["price_usd"].round(2)
        df["market_cap_usd"] = (df["market_cap_usd"]).round(5)
        df["volume_24h_usd"] = df["volume_24h_usd"].round(5)
        df["percent_change_1h_usd"] = df["percent_change_1h_usd"].round(5)
        df["percent_change_24h_usd"] = df["percent_change_24h_usd"].round(5)
        df["percent_change_7d_usd"] = df["percent_change_7d_usd"].round(5)

        return df
    except Exception as e:
        print("Error converting data types\nError:", e)
        return None
def save_cleaned_to_csv(df, filename="cleaned.csv", folder_name="logs"):

    try:
        os.makedirs(folder_name, exist_ok=True)
        file_path = os.path.join(folder_name, filename)

        write_header = True
        if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
            write_header = False

        df.to_csv(file_path, mode="a", index=False, header=write_header)
        print(f"Cleaned data saved successfully to {file_path}")

    except Exception as e:
        print("Error writing cleaned data into CSV\nError:", e)

if __name__ == "__main__":
    file_path = os.path.join("logs", "raw.csv")
    raw_data = load_raw(file_path)

    converted_data = convert_to_required_types(raw_data)
    if converted_data is not None:
        save_cleaned_to_csv(converted_data, "cleaned.csv")
