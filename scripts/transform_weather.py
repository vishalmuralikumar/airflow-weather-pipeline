import json
import logging

import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger(__name__)


def transform_weather():

    log.info("Starting weather data transformation")

    try:
        with open("/opt/airflow/data/paris_weather.json", "r") as file:
            data = json.load(file)

        df = pd.DataFrame(data["hourly"])

        df["time"] = pd.to_datetime(df["time"])

        df["city"] = "Paris"

        df["country"] = "France"

        df = df.rename(
            columns={
                "time": "timestamp",
                "temperature_2m": "temperature_c",
                "relative_humidity_2m": "humidity_percent",
                "wind_speed_10m": "wind_speed_kmh",
            }
        )

        df = df[
            [
                "timestamp",
                "city",
                "country",
                "temperature_c",
                "humidity_percent",
                "wind_speed_kmh",
            ]
        ]

        output_file = "/opt/airflow/data/paris_weather_clean.csv"

        df.to_csv(output_file, index=False)

        log.info("Weather data transformed successfully")
        log.info("Rows: %s", len(df))
        log.info("Columns: %s", list(df.columns))
        log.info("Transformed data saved to: %s", output_file)

    except Exception as error:
        log.exception(
            "Unexpected error during transformation: %s",
            error
        )
        raise


if __name__ == "__main__":
    transform_weather()