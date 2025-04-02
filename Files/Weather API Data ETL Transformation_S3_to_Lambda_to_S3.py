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
