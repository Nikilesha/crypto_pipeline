from requests import Session
from requests.exceptions import *
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime

load_dotenv()


def get_data():
    try:
        url = os.getenv("URL")
        api = os.getenv("API_KEY")
        parameters = {
            "start": "1",
            "limit": "10",
            "convert": "USD",
        }
        headers = {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": api,
        }

        session = Session()
        session.headers.update(headers)

        response = session.get(url, params=parameters, timeout=10)
        response.raise_for_status()

        data = response.json()

        if data.get("status", {}).get("error_code") != 0:
            print("API Error:", data["status"]["error_message"])
            return None

        print("Success! Fetched data")
        return data

    except Timeout:
        print("The request timed out")
    except ConnectionError:
        print("Connection error, check your network or VPN")
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except Exception as e:
        print("An unexpected error occurred:", e)
    return None


def get_required(data):
    try:
        coins = data["data"]
        extracted = []

        for coin in coins:
            usd = coin["quote"]["USD"]
            extracted.append(
                {
                    "name": coin["name"],
                    "symbol": coin["symbol"],
                    "price_usd": usd["price"],
                    "market_cap_usd": usd["market_cap"],
                    "volume_24h_usd": usd["volume_24h"],
                    "percent_change_1h_usd": usd["percent_change_1h"],
                    "percent_change_24h_usd": usd["percent_change_24h"],
                    "percent_change_7d_usd": usd["percent_change_7d"],
                    "acquired_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

        df = pd.DataFrame(extracted)
        return df

    except Exception as e:
        print("Error processing extracted data\nError:", e)
        return None


def save_to_csv(df, filename="raw.csv", folder_name="logs"):
    try:
        os.makedirs(folder_name, exist_ok=True)
        file_path = os.path.join(folder_name, filename)

        # Always overwrite and write headers
        df.to_csv(file_path, mode="w", index=False, header=True)
        print(f"Data saved successfully to {file_path}")

    except Exception as e:
        print("Error writing into CSV\nError:", e)


if __name__ == "__main__":
    api_data = get_data()
    if api_data:
        extracted_df = get_required(api_data)
        if extracted_df is not None:
            save_to_csv(extracted_df, "raw.csv")
