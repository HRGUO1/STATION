print("Initializing Flask app and S3 client...")
app = Flask(__name__)
session = boto3.Session(
        aws_access_key_id = aws_id,
        aws_secret_access_key= aws_key
    )
s3 = boto3.client('s3')

print("Setting configuration...")
S3_BUCKET_NAME = "monthly-summary"
S3_KEY_NAME = "raw-station/station.txt"
DATA_URL = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"


def stream_large_file(url):
    print("Streaming started...")
    response = requests.get(url, stream=True)
    for chunk in response.iter_content(chunk_size=5 * 1024 ** 2):
        if chunk:
            yield chunk
    print("Streaming completed.")


@app.route('/fetch_data', methods=['GET'])
def fetch_data():
    try:
        print("Initiating multipart upload...")
        mpu = s3.create_multipart_upload(Bucket=S3_BUCKET_NAME, Key=S3_KEY_NAME)
        parts = []

        print("Uploading individual parts...")
        for i, part in enumerate(stream_large_file(DATA_URL)):
            part_number = i + 1
            part_output = s3.upload_part(
                Body=part,
                Bucket=S3_BUCKET_NAME,
                Key=S3_KEY_NAME,
                PartNumber=part_number,
                UploadId=mpu['UploadId']
            )
            parts.append({'PartNumber': part_number, 'ETag': part_output['ETag']})

        print("Completing the upload...")
        s3.complete_multipart_upload(
            Bucket=S3_BUCKET_NAME,
            Key=S3_KEY_NAME,
            UploadId=mpu['UploadId'],
            MultipartUpload={'Parts': parts}
        )

        print("File uploaded successfully!")
        return jsonify({'message': 'File uploaded successfully!'}), 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({'message': str(e)}), 500


if __name__ == '__main__':
    print("Starting Flask app...")
    app.run(debug=True)
