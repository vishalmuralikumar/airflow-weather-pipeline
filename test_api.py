import requests

url = "https://api.open-meteo.com/v1/forecast"

params = {
    "latitude": 48.8566,
    "longitude": 2.3522,
    "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
    "timezone": "Europe/Paris",
}

response = requests.get(url, params=params)

print("Status Code:", response.status_code)

data = response.json()

print(data)