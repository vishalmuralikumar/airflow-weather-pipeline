import logging

import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger(__name__)


def validate_weather():

    log.info("Starting weather data validation")

    try:
        file_path = "/opt/airflow/data/paris_weather_clean.csv"

        df = pd.read_csv(file_path)

        log.info("Total rows: %s", len(df))
        log.info("Total columns: %s", len(df.columns))

        log.info("Checking missing values")
        log.info("\n%s", df.isnull().sum())

        duplicate_timestamps = df["timestamp"].duplicated().sum()
        log.info("Duplicate timestamps: %s", duplicate_timestamps)

        log.info(
            "Temperature range: %s to %s",
            df["temperature_c"].min(),
            df["temperature_c"].max()
        )

        log.info(
            "Humidity range: %s to %s",
            df["humidity_percent"].min(),
            df["humidity_percent"].max()
        )

        log.info(
            "Wind speed range: %s to %s",
            df["wind_speed_kmh"].min(),
            df["wind_speed_kmh"].max()
        )

        log.info("Data validation completed successfully")

    except Exception as error:
        log.exception(
            "Unexpected error during data validation: %s",
            error
        )
        raise


if __name__ == "__main__":
    validate_weather()