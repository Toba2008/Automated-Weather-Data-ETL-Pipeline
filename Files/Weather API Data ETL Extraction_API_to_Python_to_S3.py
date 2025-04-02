import json
import requests
import boto3
from datetime import datetime

# AWS S3 Configuration (Replace with your actual credentials)
aws_access_key = "access key"  # Replace with your AWS access key
aws_secret_key = "secret key"  # Replace with your AWS secret key
region_name = "us-east-1"  # Example: "us-east-1"
bucket_name = "weather-etl-s3-bucket-emmanuel" # Name of your S3 bucket
s3_key = "raw_data/weather_data.json"  # Path in S3

# Initialize S3 client with credentials
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name
)

# OpenWeather API Key
API_KEY = "API KEY"  # Replace with your OpenWeather API key
# Note: You can get your API key from https://openweathermap.org/appid

# Define API endpoint
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# List of cities to fetch weather data for
cities = ["London", "Glasgow", "Blackpool", "Greenock", "Lagos", "Cornwall", "Reading", "Manchester", "Birmingham", "Liverpool", 
          "Derby", "Leeds", "Sheffield", "Bristol", "Cardiff", "Swansea", "Edinburgh", "Aberdeen", "Dundee", "Inverness", 
          "Newcastle", "York", "Nottingham", "Leicester", "Coventry", "Oxford", "Cambridge", "Brighton", "Southampton", 
          "Portsmouth", "Bournemouth", "Exeter", "Plymouth", "Belfast", "Dublin"]

def fetch_weather(city):
    """Fetch current weather data for a city."""
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"  # Use "imperial" for Fahrenheit
    }

    response = requests.get(BASE_URL, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {city}: {response.status_code}")
        return None

# Fetch data for all cities
all_weather_data = []

for city in cities:
    print(f"Fetching weather data for {city}...")
    weather_data = fetch_weather(city)

    if weather_data:
        all_weather_data.append(weather_data)  # Store raw JSON data

# Convert to JSON format
raw_weather_json = json.dumps(all_weather_data, indent=4)

# Upload JSON data to S3
try:
    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=raw_weather_json,
        ContentType="application/json"
    )
    print("Weather data successfully uploaded to S3!")
except Exception as e:
    print("Error uploading data to S3:", str(e))

    # Check if file exists
try:
    response = s3.head_object(Bucket=bucket_name, Key=s3_key)
    print(f"File found! Size: {response['ContentLength']} bytes")
except Exception as e:
    print("File not found or error occurred:", str(e))
