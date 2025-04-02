import json
import requests
import boto3
from datetime import datetime
import time

# Initialize S3 client (Use IAM Role instead of hardcoded credentials)
s3 = boto3.client('s3')

# AWS S3 Configuration
bucket_name = "weather-etl-s3-bucket-emmanuel"

# OpenWeather API Key
API_KEY = "API KEY"  # Replace with your OpenWeather API key
# Note: You can get your API key from https://openweathermap.org/appid
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

cities = ["London", "Glasgow", "Blackpool", "Greenock", "Lagos", "Cornwall", "Reading", "Manchester", "Birmingham", "Liverpool", 
          "Derby", "Leeds", "Sheffield", "Bristol", "Cardiff", "Swansea", "Edinburgh", "Aberdeen", "Dundee", "Inverness", 
          "Newcastle", "York", "Nottingham", "Leicester", "Coventry", "Oxford", "Cambridge", "Brighton", "Southampton", 
          "Portsmouth", "Bournemouth", "Exeter", "Plymouth", "Belfast", "Dublin"]

def lambda_handler(event, context):
    """AWS Lambda entry point"""

    def fetch_weather(city):
        """Fetch current weather data for a city."""
        params = {"q": city, "appid": API_KEY, "units": "metric"}
        response = requests.get(BASE_URL, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching data for {city}: {response.status_code}")
            return None

    # Generate a unique S3 key for each execution
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    unique_id = int(time.time() * 1000)  # Adds milliseconds
    s3_key = f"raw_data_from_lambda/to_processed/lambda_raw_weather_data_{timestamp}.json"

    all_weather_data = []

    for city in cities:
        weather_data = fetch_weather(city)
        if weather_data:
            all_weather_data.append(weather_data)

    raw_weather_json = json.dumps(all_weather_data, indent=4)

    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=raw_weather_json,
            ContentType="application/json"
        )
                
        print(f"Weather data uploaded to S3: {s3_key}")
        return {"status": "success", "message": "Weather data uploaded to S3"}
    
    except Exception as e:
        print(f"Error uploading data to S3: {str(e)}")
        return {"status": "error", "message": str(e)}
