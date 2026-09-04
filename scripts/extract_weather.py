import json
import logging
from pathlib import Path

import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger(__name__)


def extract_weather():
    log.info("Starting weather data extraction")

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": 48.8566,
        "longitude": 2.3522,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m"
        ],
        "timezone": "Europe/Paris"
    }

    try:
        response = requests.get(
            url,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        data = response.json()

        output = Path("/opt/airflow/data/paris_weather.json")
        output.parent.mkdir(parents=True, exist_ok=True)

        with open(output, "w") as file:
            json.dump(data, file, indent=4)

        log.info("Weather data extracted successfully")
        log.info("Raw weather data saved to %s", output)

    except requests.RequestException as error:
        log.error("Weather API request failed: %s", error)
        raise

    except Exception as error:
        log.exception("Unexpected error during extraction: %s", error)
        raise


if __name__ == "__main__":
    extract_weather()