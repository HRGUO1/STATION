from flask import Flask, jsonify
import requests
import boto3
import hashlib

aws_id = "AKIA6IMSXULXZG7KO5IU"
aws_key = "Kv1eFQBT4HWx9YdZY3xZyW/irIRYgHSrBXmolNWP"
buk = "monthly-summary"

app = Flask(__name__)
print("Setting up S3 client.")

# Assuming aws_id and aws_key are defined or imported from somewhere
s3 = boto3.client('s3', aws_access_key_id=aws_id, aws_secret_access_key=aws_key)

def fetch_gist_data():
    print("Fetching data from ncdc.")
    url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
    response = requests.get(url)
    print("Data fetched from ncdc.")
    return response.text

def upload_to_s3(data, bucket_name, object_name):
    print(f"Uploading data to S3 bucket: {bucket_name}, object: {object_name}.")
    s3.put_object(Body=data, Bucket=bucket_name, Key=object_name)
    print("Data uploaded to S3.")

@app.route('/', methods=['GET'])
def home():
    return "Welcome to the Flask app!"

@app.route('/fetch_and_upload', methods=['GET'])
def fetch_and_upload():
    print("Processing /fetch_and_upload request.")
    data = fetch_gist_data()

    # Assuming 'buk' and 'object_name' are defined or imported from somewhere
    upload_to_s3(data, buk, 'raw/station.txt')

    print("Completed /fetch_and_upload request.")
    return jsonify({"message": "Data fetched and uploaded successfully!"})

if __name__ == "__main__":
    print("Starting Flask app.")
    app.run(debug=True)
