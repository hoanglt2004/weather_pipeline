-- ===============================
-- DIMENSION TABLES
-- ===============================

-- 1. dim_city
CREATE TABLE IF NOT EXISTS dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(255) NOT NULL UNIQUE
);

-- 2. dim_weather_condition
CREATE TABLE IF NOT EXISTS dim_weather_condition (
    condition_id SERIAL PRIMARY KEY,
    weather VARCHAR(255),
    weather_description VARCHAR(255),
    weather_category VARCHAR(255)
);

-- 3. dim_date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id BIGINT PRIMARY KEY,
    date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT NOT NULL
);

-- ===============================
-- FACT TABLE
-- ===============================

-- 4. fact_weather
CREATE TABLE IF NOT EXISTS fact_weather (
    weather_id BIGSERIAL PRIMARY KEY,

    city_id INT REFERENCES dim_city(city_id),
    condition_id INT REFERENCES dim_weather_condition(condition_id),
    date_id INT REFERENCES dim_date(date_id),

    -- Bổ sung timestamp chính xác
    timestamp_utc TIMESTAMP NOT NULL,

    -- Metrics
    temperature DOUBLE PRECISION,
    feels_like DOUBLE PRECISION,
    temp_min DOUBLE PRECISION,
    temp_max DOUBLE PRECISION,
    humidity INT,
    pressure INT,
    wind_speed DOUBLE PRECISION,
    feels_like_diff DOUBLE PRECISION,

    -- Derived
    temp_category VARCHAR(255),
    is_rain BOOLEAN,
    wind_level VARCHAR(255),

    -- KEY QUAN TRỌNG
    UNIQUE (city_id, timestamp_utc)
);



-- ===============================
-- DROP TABLE
-- ===============================

-- DROP TABLE IF EXISTS dim_city CASCADE
-- DROP TABLE IF EXISTS dim_weather_condition CASCADE
-- DROP TABLE IF EXISTS dim_date CASCADE
-- DROP TABLE IF EXISTS fact_weather CASCADE
