import glob
import json
import logging
import os
import time

import pandas as pd
import psutil

logging.basicConfig(level=logging.INFO)


def load_raw_files(city, date):
    json_files_list = []
    hours_path = os.path.join("data/raw", city, date)
    for hour_folder_name in os.listdir(hours_path):
        for json_file in glob.glob(
            os.path.join(hours_path, hour_folder_name, "*.json")
        ):
            json_file = json_file.replace("\\", "/")
            json_files_list.append(json_file)
    return json_files_list


def parse_weather_json(json_data, city):
    weather_main = None
    weather_desc = None
    if (
        "weather" in json_data
        and isinstance(json_data["weather"], list)
        and len(json_data["weather"]) > 0
    ):
        weather_main = json_data["weather"][0].get("main")
        weather_desc = json_data["weather"][0].get("description")

    result = {
        "city": city,
        "temperature": json_data.get("main", {}).get("temp"),
        "feels_like": json_data.get("main", {}).get("feels_like"),
        "temp_min": json_data.get("main", {}).get("temp_min"),
        "temp_max": json_data.get("main", {}).get("temp_max"),
        "humidity": json_data.get("main", {}).get("humidity"),
        "pressure": json_data.get("main", {}).get("pressure"),
        "weather": weather_main,
        "weather_description": weather_desc,
        "wind_speed": json_data.get("wind", {}).get("speed"),
        "timestamp": json_data.get("dt"),
    }
    return result


def feature_engineering(df):
    # Them cot feels_like_diff
    df["feels_like_diff"] = df["feels_like"] - df["temperature"]
    # Them cot temp_category
    df["temp_category"] = df["temperature"].apply(
        lambda x: "hot" if x > 30 else ("warm" if 20 <= x <= 30 else "cold")
    )
    # Them cot is_rain
    df["is_rain"] = df["weather_description"].str.contains("rain", case=False, na=False)
    # Them cot wind_level
    df["wind_level"] = df["wind_speed"].apply(lambda x: "high" if x > 10 else "normal")

    # Them cot weather_category
    def map_weather_category(main):
        if pd.isna(main):
            return "unknown"
        main = main.lower()
        if main == "clear":
            return "good"
        elif main == "clouds":
            return "cloudy"
        elif main in ["rain", "drizzle"]:
            return "rain"
        elif main == "snow":
            return "snow"
        elif main in ["thunderstorm"]:
            return "storm"
        elif main in ["mist", "fog", "haze", "smoke"]:
            return "fog"
        else:
            return "other"

    df["weather_category"] = df["weather"].apply(map_weather_category)

    return df


def validate_data(df):
    logging.info("🔍 Checking data quality...")

    # Cac cot bat buoc khong duoc null
    required_cols = [
        "city",
        "temperature",
        "humidity",
        "weather",
        "wind_speed",
        "timestamp",
    ]
    for col in required_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            logging.warning(f"[DQ] Col '{col}' has {null_count}  NULL values!")
    # Temperature range check
    invalid_temp = df[(df["temperature"] < -50) | (df["temperature"] > 60)]
    if not invalid_temp.empty:
        logging.warning(
            f"[DQ] {len(invalid_temp)} records have unusual temperature [-50, 60]!"
        )
    # Humidity
    invalid_humidity = df[(df["humidity"] < 0) | (df["humidity"] > 100)]
    if not invalid_humidity.empty:
        logging.warning(f"[DQ] {len(invalid_humidity)} have humidity out of [0,100]")
    # Pressure
    invalid_pressure = df[df["pressure"] <= 0]
    if not invalid_pressure.empty:
        logging.warning(f"[DQ] {len(invalid_pressure)} have pressure <= 0")
    # wind_speed toi thieu = 0
    invalid_wind = df[df["wind_speed"] < 0]
    if not invalid_wind.empty:
        logging.warning(f"[DQ] {len(invalid_wind)} record have wind_speed < 0!")

    logging.info("✔ Data Quality Check Sucessfully (non-blocking).")
    return df


def tranform_city(city, date):
    logging.info(f"===== 🔧 START TRANSFORM for {city} - {date} =====")
    start = time.time()

    json_files_list = load_raw_files(city, date)
    if not json_files_list:
        logging.warning(f"Not found raw file for {city} on {date}")
        return None
    logging.info(f"📄 Found {len(json_files_list)} raw files for {city}")

    rows = []
    for file in json_files_list:
        logging.info(f"➡️ Parsing file: {file}")
        try:
            weather_data = json.load(open(file))
            record = parse_weather_json(weather_data, city)
        except json.JSONDecodeError:
            logging.error(f"❌ Parse failed: {file}")
            continue
        rows.append(record)

    if not rows:
        logging.warning(f"❗ No valid JSON for {city}")
        return None

    weather_df = pd.DataFrame(rows)
    logging.info(f"📌 Parsed records: {len(weather_df)}")

    # Chuyen doi timestamp thanh datetime chuan iso va them hai cot date va hour
    weather_df["timestamp_utc"] = pd.to_datetime(weather_df["timestamp"], unit="s")
    weather_df["date"] = weather_df["timestamp_utc"].dt.strftime("%Y-%m-%d")
    weather_df["hour"] = weather_df["timestamp_utc"].dt.hour.astype(int)

    # Sap xep thu tu cac cot
    ordered_cols = [
        "city",
        "temperature",
        "feels_like",
        "temp_min",
        "temp_max",
        "humidity",
        "pressure",
        "weather",
        "weather_description",
        "wind_speed",
        "timestamp",
        "timestamp_utc",
        "date",
        "hour",
    ]
    weather_df = weather_df[ordered_cols]
    # Data quality check va enrich df
    weather_df = validate_data(weather_df)
    weather_df = feature_engineering(weather_df)

    # Tao thu muc output clean
    with open("config/config.json", "r") as f:
        cfg = json.load(f)
        clean_base = cfg["clean_path"]
    out_dir = os.path.join(clean_base, city, date)
    os.makedirs(out_dir, exist_ok=True)

    # Ghi parquet
    out_path = os.path.join(out_dir, "weather_clean_enriched.parquet")
    weather_df.to_parquet(out_path, index=False, engine="pyarrow")

    logging.info(f"💾 Saved Parquet: {out_path}")
    logging.info(
        f"💾 Memory used: {psutil.Process().memory_info().rss / 1024**2:.2f} MB"
    )
    logging.info(f"⏱ Transform time: {time.time() - start:.2f}s")
    logging.info(f"===== ✅ END TRANSFORM for {city} =====")

    return out_path

    #  #Ghi csv
    # out_path = os.path.join(out_dir,'weather_clean.csv')
    # weather_df.to_csv(out_path,index=False)
    # logging.info(f'Da tao file clean: {out_path}')
    # return out_path
