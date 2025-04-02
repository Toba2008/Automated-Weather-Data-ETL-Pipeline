# Automated-Weather-Data-ETL-Pipeline

## Introduction

Hello! My name is Oluwatoba Emmanuel Odeyemi, and this is my **Automated Weather Data ETL Pipeline** project. This project aims to extract weather data from the **OpenWeather API**, transform it into a structured format, and store it in **AWS S3** for further analytics using **AWS Glue and AWS Athena**.

This automated pipeline ensures that fresh weather data is retrieved every 24 hours without manual intervention. I developed this project to demonstrate my expertise in **AWS Data Engineering** by integrating various AWS services to process, store, and analyze weather data efficiently.

## Problem Statement

Weather data is crucial for analytics, forecasting, and decision-making. However, fetching and storing weather data manually is inefficient and time-consuming, especially when dealing with multiple locations. My solution is an **ETL Extract, Transform, Load) pipeline** that automates the process of fetching weather data for multiple cities, transforming it into a structured format, and making it readily available for querying.

This pipeline ensures that I can:

*  Continuously fetch real-time weather data from the OpenWeather API for multiple locations.

*  Store raw data in AWS S3 for archival purposes.

*  Transform and structure the data into a readable format.

*  Use AWS Glue to catalog the data for efficient querying.

*  Query the structured data using AWS Athena, enabling quick analysis and insights on city-specific weather conditions.


## Dataset/API Information

For this project, I used the **OpenWeather API** to fetch real-time weather data for multiple cities. The API provides detailed weather information, including:

*  **City/Location** (Name of the city where weather data is retrieved)
*  **Temperature** (in Celsius)
*  **Humidity** (percentage)
*  **Wind speed** (in meters per second)
*  **Pressure** (in hPa)
*  **Weather condition descriptions** (e.g., cloudy, sunny, rainy)

To use this API, you need an API key from [OpenWeather](https://openweathermap.org/api).

## Project Architecture
Below is the architecture diagram illustrating the flow of data in this ETL pipeline:

![Automated Weather API ETL Pipeline2](https://github.com/user-attachments/assets/7c8c48f8-a484-4b53-9655-27129d79c41a)

### Architecture Overview:

This project follows a structured workflow:

1.  **Data Extraction**: Fetching weather data using a Python script.
2.  **Automated Data Extraction**: Deploying an AWS Lambda function to automate the extraction process every 24 hours.
3.  **Data Storage**: Saving raw JSON data to an S3 bucket.
4.  **Data Transformation**: Processing raw JSON data into a structured CSV format using a second Lambda function.
5.  **Data Storage (Post-Transformation)**: Saving transformed data into a separate S3 folder.
6.  **Data Cataloging**: Using AWS Glue to create a catalog for efficient querying.
7.  **Querying Data**: Using AWS Athena to analyze weather trends per location.

## Technologies Used

I built this project using the following tools and technologies:

*  **Python**: To write scripts for data extraction and transformation.
*  **AWS Lambda**: To automate the extraction and transformation processes.
*  **AWS S3**: To store raw and processed weather data.
*  **AWS CloudWatch**: To monitor Lambda function execution.
*  **AWS Glue**: To create a Data Catalog and automate schema inference.
*  **AWS Athena**: To query the processed data efficiently.

## Implementation Steps

The project was fully executed on the AWS console, covering data extraction, transformation, and automation. However, I initially extracted the data from its source and stored it directly in Amazon S3 from my local machine before implementing full automation.

**Part 1: Extract & Store Data from Python to Data Warehouse**
  1.  Extracting Data from OpenWeather API:
      I wrote a Python script to fetch weather data from multiple cities and save it to an S3 bucket. The script makes API calls, extracts relevant data, and uploads it as a JSON file to the **raw_data** folder in S3.
  2.  Loaded the raw weather data into an S3 bucket directly from the Python environment using the following code.
   
```
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

```
![weather-etl-s3-bucket](https://github.com/user-attachments/assets/0ad43ae5-a3a3-4fc8-9d1e-04630a3e1679)


**Part 2: Extract & Deploy AWS Lambda Code**

  1.  Automating Data Extraction with AWS Lambda:
      To automate data extraction, I deployed the Python script to AWS Lambda and scheduled it to run every 24 hours using AWS EventBridge. The Lambda function fetches weather data and stores it in S3 automatically.
  2.  Defined environment variables, such as access and secret keys, for security purposes within the function script.
  3.  Created a Lambda layer for the requests library.
  4.  Configured the runtime environment.
  5.  Updated the IAM role policy by adding necessary permissions and attaching policies.
  6.  Added an automatic trigger in AWS Lambda.
  7.  Used Amazon EventBridge to trigger the Lambda function every 24 hours, ensuring the API data is fetched automatically at regular intervals.

```
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

```
![Weather_api_data_extract](https://github.com/user-attachments/assets/f8fbf351-92de-41a4-95d0-ebc819e01624)


**Part 3: Write & Deploy Transformation Code on the Lambda Console**

  1.  Transforming Data into a Structured Format:
      A second Lambda function processes the raw JSON data, extracting key attributes like city, temperature, humidity, and timestamp, and converts it into a structured CSV format.
  2.  Defined environment variables.
  3.  Created a Lambda layer.
  4.  Configured the runtime environment.
  5.  Deployed and tested the Lambda function:
      *  Converted raw data into a DataFrame.
      *  Formatted the datetime field.
      *  Removed duplicate entries.
  6.  Added an S3 bucket trigger to store transformed data:
      *  Each time new raw data is extracted from the OpenWeather API, the transformed data is automatically stored in a new S3 location.

```
import json
import boto3
import pandas as pd
import io
from datetime import datetime

# Initialize S3 client
s3 = boto3.client('s3')

# AWS S3 Configuration
bucket_name = "weather-etl-s3-bucket-emmanuel"
raw_data_prefix = "raw_data_from_lambda/to_processed/"
transformed_data_prefix = "transformed_data/"

def get_latest_s3_file(bucket, prefix):
    """Retrieve the latest uploaded file from the S3 bucket."""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if "Contents" not in response:
        raise Exception("No files found in the raw data folder!")

    # Sort files by last modified date
    sorted_files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
    
    return sorted_files[0]["Key"]  # Return the latest file's key

def lambda_handler(event, context):
    """
    AWS Lambda function to:
    - Read raw weather data from S3
    - Transform key attributes
    - Store the processed data in the 'transformed_data' folder in S3
    """
    try:
        # Fetch the latest raw data file
        latest_s3_key = get_latest_s3_file(bucket_name, raw_data_prefix)
        print(f"Fetching raw data from: {latest_s3_key}")

        obj = s3.get_object(Bucket=bucket_name, Key=latest_s3_key)
        raw_data = json.loads(obj["Body"].read().decode("utf-8"))

        # Extract relevant weather details
        transformed_data = []
        for city_data in raw_data:
            transformed_data.append({
                "City": city_data.get("name", ""),
                "Temperature_C": city_data["main"].get("temp", None),
                "Feels_Like_C": city_data["main"].get("feels_like", None),
                "Humidity": city_data["main"].get("humidity", None),
                "Pressure_hPa": city_data["main"].get("pressure", None),
                "Wind_Speed_mps": city_data["wind"].get("speed", None),
                "Wind_Degree": city_data["wind"].get("deg", None),
                "Weather_Description": city_data["weather"][0].get("description", ""),
                "Timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            })

        # Convert to Pandas DataFrame
        df = pd.DataFrame(transformed_data)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])

        # Print transformed data
        print(f"Transformed data:\n{df}")
        print(f"Transformed data types:\n{df.dtypes}")

        # Save DataFrame as CSV in-memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Define S3 key for transformed data
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        transformed_s3_key = f"{transformed_data_prefix}transformed_weather_data_{timestamp}.csv"

        # Upload transformed data to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=transformed_s3_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )

        print(f"Transformed data uploaded to S3: {transformed_s3_key}")
        return {"status": "success", "message": "Data transformation and upload completed successfully."}

    except Exception as e:
        print(f"Error in Lambda execution: {str(e)}")
        return {"status": "error", "message": str(e)}

```
![Weather_api_data_transformation_load_function](https://github.com/user-attachments/assets/734be751-a925-4aa7-879a-63b425db9079)


**Part 4: Build Analytics Tables Using AWS Glue & Athena**
  1.  Querying Data Using AWS Glue and Athena:
      *  **AWS Glue**: I created a crawler to infer the schema from the transformed data and store it in the Glue Data Catalog.
      *  **AWS Athena**: I used Athena to write SQL queries on the weather dataset stored in S3.
  2.  Created an AWS Glue Crawler to scan the transformed data and populate an AWS Glue Data Catalog table.
  3.  Ran SQL queries in AWS Athena to analyze the data.
  4.  Automated the AWS Glue Crawler to run periodically:
      *  This ensures that the AWS Glue Data Catalog remains updated with the latest data, scheduled to run at fixed intervals.

![Athena Table Query Result](https://github.com/user-attachments/assets/e0a78dc9-fd7c-484c-888e-08e8094cffb9)


## Conclusion and Future Improvements

This project demonstrates how I built an end-to-end ETL pipeline for processing weather data using AWS services. The pipeline runs automatically, ensuring that fresh weather data is always available for analysis.

**Future Enhancements:**

1.  **Error Handling**: Implement robust error handling for API failures.
2.  **Real-Time Processing**: Extend the pipeline to support streaming data using AWS Kinesis.
3.  **Additional Data Sources**: Integrate more weather-related APIs for deeper insights.

This project does not only showcase my ability to work with **AWS data services**, but also demonstrates my **expertise in ETL pipeline development** and **cloud automation**. If you're interested in learning more or collaborating, feel free to connect with me!


## Contact

If you have any questions, feedback, or collaboration ideas, feel free to reach out to me via LinkedIn or GitHub.

**Thanks for checking out my project!**

