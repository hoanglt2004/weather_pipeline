import logging
import os
import time

import pandas as pd
import psycopg2
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)


def connect_postgres():
    try:
        # 1) Load biến môi trường từ .env
        load_dotenv()
        host = os.getenv("POSTGRES_HOST")
        port = os.getenv("POSTGRES_PORT")
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        dbname = os.getenv("POSTGRES_DB")

        # 2) Kết nối Postgres
        conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, database=dbname
        )

        cursor = conn.cursor()
        logging.info("Kết nối PostgreSQL thành công.")
        return conn, cursor

    except Exception as e:
        logging.error(f"Lỗi kết nối PostgreSQL: {e}")
        return None, None


def load_parquet_to_df(parquet_path):
    """
    Load file Parquet vào Pandas DataFrame.
    """
    try:
        df = pd.read_parquet(parquet_path)
        logging.info(f"Đã load Parquet thành công: {parquet_path}")
        return df

    except FileNotFoundError:
        logging.error(f"Không tìm thấy file Parquet: {parquet_path}")
        return None

    except Exception as e:
        logging.error(f"Lỗi load Parquet: {e}")
        return None


def get_or_create_city(cursor, conn, city_name):
    """
    Lấy city_id từ dim_city.
    Nếu chưa tồn tại → INSERT → trả về city_id mới.
    """

    # Kiem tra city da ton tai chua
    try:
        cursor.execute(
            "SELECT city_id FROM dim_city WHERE city_name = %s;", (city_name,)
        )
        result = cursor.fetchone()
        if result:
            city_id = result[0]
            # logging.info(f"City '{city_name}' da ton tai voi ID {city_id}")
            return city_id
    except Exception as e:
        logging.error(f"Loi SELECT dim_city: {e}")
        return None

    # Neu chua co thi INSERT
    try:
        cursor.execute(
            "INSERT INTO dim_city (city_name) VALUES (%s) RETURNING city_id;",
            (city_name,),
        )
        new_id = cursor.fetchone()[0]
        conn.commit()

        logging.info(f"Da tao city moi '{city_name}' voi ID {new_id}")
        return new_id
    except Exception as e:
        conn.rollback()
        logging.error(f"Loi INSERT dim_city: {e}")
        return None


def get_or_create_condition(cursor, conn, weather, description, category):
    """
    Lấy condition_id từ dim_weather_condition.
    Nếu chưa tồn tại → INSERT → RETURN NEW ID.
    """
    # Check xem dieu kien thoi tiet da ton tai chua
    try:
        cursor.execute(
            """
            SELECT condition_id
            FROM dim_weather_condition
            WHERE weather = %s AND weather_description = %s and weather_category = %s;
            """,
            (weather, description, category),
        )
        result = cursor.fetchone()
        # Neu ton tai tra ve ID
        if result:
            condition_id = result[0]
            # logging.info(f"Điều kiện thời tiết đã tồn tại với ID {condition_id}")
            return condition_id
    except Exception as e:
        logging.error(f"Lỗi SELECT dim_weather_condition: {e}")
        return None

    # Neu chua co thi INSERT
    try:
        cursor.execute(
            """
            INSERT INTO dim_weather_condition (weather, weather_description, weather_category)
            VALUES (%s, %s, %s)
            RETURNING condition_id;
        """,
            (weather, description, category),
        )

        new_id = cursor.fetchone()[0]
        conn.commit()

        logging.info(f"Đã tạo weather condition mới với ID {new_id}")
        return new_id

    except Exception as e:
        conn.rollback()
        logging.error(f"Lỗi INSERT dim_weather_condition: {e}")
        return None


def get_or_create_date(cursor, conn, date_str, hour):
    """
    Version tối ưu: date_id = YYYYMMDDHH
    """

    year = int(date_str[:4])
    month = int(date_str[5:7])
    day = int(date_str[8:10])

    date_id = int(date_str.replace("-", "") + f"{hour:02}")

    # 1) Kiểm tra đã tồn tại chưa
    try:
        cursor.execute("SELECT date_id FROM dim_date WHERE date_id = %s;", (date_id,))
        result = cursor.fetchone()

        if result:
            # logging.info(f"Đã tồn tại date_id {result[0]}")
            return date_id  # đã có
    except Exception as e:
        logging.error(f"Lỗi SELECT dim_date: {e}")
        return None

    # 2) Chưa có → INSERT
    try:
        cursor.execute(
            """
            INSERT INTO dim_date (date_id, date, year, month, day, hour)
            VALUES (%s, %s, %s, %s, %s, %s);
        """,
            (date_id, date_str, year, month, day, hour),
        )

        conn.commit()
        logging.info(f"Đã tạo date_id mới {date_id}")
        return date_id

    except Exception as e:
        conn.rollback()
        logging.error(f"Lỗi INSERT dim_date: {e}")
        return None


BATCH_SIZE = 50
batch_counter = 0


def insert_fact_weather(cursor, conn, row, city_id, cond_id, date_id):
    """
    Insert hoặc Update 1 bản ghi vào fact_weather (UPSERT).
    Version BATCH: commit sau mỗi BATCH_SIZE bản ghi.
    """
    global batch_counter

    try:
        cursor.execute(
            """
            INSERT INTO fact_weather (
                city_id, condition_id, date_id,timestamp_utc,
                temperature, feels_like, temp_min, temp_max,
                humidity, pressure, wind_speed,
                feels_like_diff, temp_category, is_rain, wind_level
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city_id, timestamp_utc)
            DO UPDATE SET
                condition_id     = EXCLUDED.condition_id,
                temperature      = EXCLUDED.temperature,
                feels_like       = EXCLUDED.feels_like,
                temp_min         = EXCLUDED.temp_min,
                temp_max         = EXCLUDED.temp_max,
                humidity         = EXCLUDED.humidity,
                pressure         = EXCLUDED.pressure,
                wind_speed       = EXCLUDED.wind_speed,
                feels_like_diff  = EXCLUDED.feels_like_diff,
                temp_category    = EXCLUDED.temp_category,
                is_rain          = EXCLUDED.is_rain,
                wind_level       = EXCLUDED.wind_level;
        """,
            (
                city_id,
                cond_id,
                date_id,
                row.get("timestamp_utc"),
                row.get("temperature"),
                row.get("feels_like"),
                row.get("temp_min"),
                row.get("temp_max"),
                row.get("humidity"),
                row.get("pressure"),
                row.get("wind_speed"),
                row.get("feels_like_diff"),
                row.get("temp_category"),
                row.get("is_rain"),
                row.get("wind_level"),
            ),
        )

        # tăng bộ đếm batch
        batch_counter += 1

        # commit mỗi BATCH_SIZE bản ghi
        if batch_counter % BATCH_SIZE == 0:
            conn.commit()
            logging.info(f"Đã commit batch {batch_counter}")

    except Exception as e:
        conn.rollback()
        logging.error(f"Lỗi UPSERT fact_weather (rollback batch): {e}")


def finalize_fact_batch(conn):
    try:
        conn.commit()
        logging.info("Commit batch cuối.")
    except Exception as e:
        logging.error(f"Lỗi final commit: {e}")


def load_to_dwh(parquet_file):
    """
    Load dữ liệu clean parquet -> PostgreSQL DWH.
    Gồm:
        - dim_city
        - dim_weather_condition
        - dim_date
        - fact_weather
    """

    logging.info(f"===== 📥 START LOAD for {parquet_file} =====")
    start = time.time()

    # 1) Load parquet -> DataFrame
    logging.info(f"📄 Loading Parquet: {parquet_file}")
    df = load_parquet_to_df(parquet_file)
    if df is None or df.empty:
        logging.error("❌ Cannot load to DWH with an empty dataframe.")
        return

    # 2) Kết nối PostgreSQL
    conn, cursor = connect_postgres()
    if not conn:
        logging.error("❌ Unable to connect to PostgreSQL")
        return

    logging.info(f"📌 Records to load: {len(df)}")

    # 3) Loop từng dòng trong DataFrame
    for _, row in df.iterrows():

        # ---- Dimension City ----
        city_id = get_or_create_city(cursor, conn, row["city"])

        # ---- Dimension Weather ----
        cond_id = get_or_create_condition(
            cursor,
            conn,
            row["weather"],
            row["weather_description"],
            row["weather"],  # nếu bạn có category riêng thì thay vào đây
        )

        # ---- Dimension Date ----
        date_id = get_or_create_date(
            cursor, conn, row["date"], row["hour"]  # "YYYY-MM-DD"  # "HH"
        )

        # ---- FACT TABLE ----
        insert_fact_weather(cursor, conn, row, city_id, cond_id, date_id)

    # 4) Commit batch còn lại
    finalize_fact_batch(conn)

    # 5) Đóng connection
    conn.close()
    logging.info("💾 Load completed successfully")
    logging.info(f"⏱ Load time: {time.time() - start:.2f}s")
    logging.info(f"===== ✅ END LOAD for {parquet_file} =====")
