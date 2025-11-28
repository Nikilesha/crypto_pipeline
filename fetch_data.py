from requests import Session
from requests.exceptions import *
from dotenv import load_dotenv
import pprint
import os
import pandas as pd
from datetime import datetime

load_dotenv()


def get_data():
    try:
        url = os.getenv("URL")
        parameters = {
            "start": "1",
            "limit": "10",
            "convert": "INR,USD",
        }
        headers = {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": "b54bcf4d-1bca-4e8e-9a24-22ff2c3d462c",
        }

        session = Session()
        session.headers.update(headers)
    except Exception as e:
        print("Error getting api data\nError:", e)

    try:
        response = session.get(url, params=parameters, timeout=10)

        data = response.json()
        response.raise_for_status()

        if data.get("status", {}).get("error_code") != 0:
            print("API Error:", data["status"]["error_message"])
        else:
            print("Success! fetched data")

    except Timeout:
        print("The request timed out")
    except ConnectionError:
        print("Connection error, check your network or vpn")
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except Exception as e:
        print("An error occurred:", e)

    return data


def get_required(data):
    try:
        coins = data["data"]
        extracted = []
        for coin in coins:
            inr_quote = coin["quote"]["INR"]
            usd_quote = coin["quote"]["USD"]
            extracted.append(
                {
                    "name": coin["name"],
                    "symbol": coin["symbol"],
                    "price_inr": inr_quote["price"],
                    "price_usd": usd_quote["price"],
                    "market_cap_inr": inr_quote["market_cap"],
                    "market_cap_usd": usd_quote["market_cap"],
                    "volume_24h_inr": inr_quote["volume_24h"],
                    "volume_24h_usd": usd_quote["volume_24h"],
                    "percent_change_1h_inr": inr_quote["percent_change_1h"],
                    "percent_change_1h_usd": usd_quote["percent_change_1h"],
                    "percent_change_24h_inr": inr_quote["percent_change_24h"],
                    "percent_change_24h_usd": usd_quote["percent_change_24h"],
                    "percent_change_7d_inr": inr_quote["percent_change_7d"],
                    "percent_change_7d_usd": usd_quote["percent_change_7d"],
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
        df = pd.DataFrame(extracted)
        return df
    except Exception as e:
        print("Error processing data\nError:", e)


def save_to_csv(df, filename="raw.csv", folder_name="raw_logs"):
    try:
        os.makedirs(folder_name, exist_ok=True)
        file_path = os.path.join(folder_name, filename) 

        file_exists = os.path.isfile(file_path)

        df.to_csv(
            file_path, mode="a", index=False, header=not file_exists
        )

        print(f"Data saved successfully to {file_path}")
    except Exception as e:
        print("Error writing into CSV\nError:", e)


if __name__ == "__main__":
    api_data = get_data()
    if api_data:
        extracted = get_required(api_data)
        save_to_csv(extracted, "raw.csv")
