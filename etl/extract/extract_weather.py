import json
import logging
import os
import time
from datetime import datetime

import requests

logging.basicConfig(level=logging.INFO)


# Tao url api cho tung thanh pho
def build_api_url(city):
    try:
        with open("config/config.json", "r") as f:
            cfg = json.load(f)
        api_url = (
            "https://api.openweathermap.org/data/2.5/weather?q="
            + city
            + "&appid="
            + cfg["api_key"]
            + "&units=metric"
        )
        return api_url
    except Exception as e:
        print(f"Error: {e}")
        return ""


# Goi api va nhan json
def fetch_weather(city):
    api_url = build_api_url(city)
    try:
        response = requests.get(api_url, headers={"accept": "application/json"})
        if response.status_code != 200:
            logging.error(f"API error: status={response.status_code}")
            return None
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error: {e}")
        return None


# Tao duong dan luu du lieu vao data/raw. Dong vai tro nhu Data Lake
def build_raw_path(city):
    # Lay timestamp hien tai
    now = datetime.now()
    # Format thanh day + hour
    day = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    # Tao cau truc folder
    with open("config/config.json", "r") as f:
        cfg = json.load(f)
        raw_base = cfg["raw_path"]
    full_path = os.path.join(raw_base, city, day, hour)
    os.makedirs(full_path, exist_ok=True)
    return full_path


def store_raw(json_data, city):
    raw_folder_path = build_raw_path(city)

    # Lay timestamp de tao ten file
    now = datetime.now()
    file_name = f"weather_{now.strftime('%Y-%m-%d-%H-%M')}.json"
    # Full path den file
    file_path = os.path.join(raw_folder_path, file_name)
    file_path = file_path.replace("\\", "/")

    # Ghi json vao file
    with open(file_path, "w", encoding="UTF8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)
    logging.info(f"{file_name} saved to {file_path}")
    return file_path


def extract_all_cities():
    logging.info("===== 🚀 START EXTRACT =====")
    start = time.time()

    # Doc file config.json
    try:
        with open("config/config.json", "r") as f:
            config = json.load(f)
            cities = config.get("cities", [])
    except Exception as e:
        logging.error(f"Cannot read config.json: {e}")
        return []

    save_files = []
    # Loop tung city
    for city in cities:
        logging.info(f"Fetching data for: {city}")
        weather_json = fetch_weather(city)
        if weather_json is None:
            logging.error(f"Cannot fetch data for {city}, skipped.")
            continue

        file_path = store_raw(weather_json, city)
        save_files.append(file_path)
    logging.info(f"📌 Extracted {len(save_files)} raw files")
    logging.info(f"⏱ Extract time: {time.time() - start:.2f}s")
    logging.info("===== ✅ END EXTRACT =====")
    return save_files


if __name__ == "__main__":
    extract_all_cities()
