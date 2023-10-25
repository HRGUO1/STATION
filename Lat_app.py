from flask import Flask, Response, jsonify, request
import boto3
import pyarrow.parquet as pq
import pandas as pd
from io import BytesIO
from pyspark.sql import SparkSession
import logging

# Logging
logging.basicConfig(level=logging.INFO)


# Initialize app
app = Flask(__name__)
s3 = boto3.client('s3', aws_access_key_id=aws_id, aws_secret_access_key=aws_key)

# Spark Config
spark = SparkSession.builder \
    .appName("app") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_id)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_key)

# Function to fetch data from S3
def fetch_data_from_s3(bucket_name, file_path):
    df = spark.read.parquet(f"s3a://{bucket_name}/{file_path}")
    logging.info(f"Fetching data from bucket: {bucket_name}, file_path: {file_path}")
    return df

# Goal 1
@app.route('/getAverageSeasonalTemperature', methods=['GET'])
def get_average_seasonal_temperature():
    logging.info("Received request for get_average_seasonal_temperature")
    try:
        file_path = 'Processed/seasonal_avg_demo.parquet'
        df = fetch_data_from_s3(buk, file_path)
        pandas_df = df.toPandas()
        html_content = pandas_df.to_html(classes='dataframe', index=False)

        return Response("""
        <html>
            <head>
                <title>Average Seasonal Temperature</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        margin: 20px;
                        background-color: #f3f3f3;
                    }}
                    h1 {{
                        text-align: center;
                        color: #333366;
                    }}
                    table {{
                        width: 100%;
                        border-collapse: collapse;
                        text-align: center;
                    }}
                    th, td {{
                        border: 1px solid #ccc;
                        padding: 8px;
                        text-align: center;
                    }}
                    th {{
                        background-color: #f2f2f2;
                    }}
                    tr:nth-child(even) {{
                        background-color: #f2f2f2;
                    }}
                </style>
            </head>
            <body>
                <h1>Average Seasonal Temperature</h1>
                <table id="myTable">
                    {}
                </table>
            </body>
        </html>
        """.format(html_content), content_type='text/html')

    except Exception as e:
        from flask import jsonify  # Make sure you import jsonify
        return jsonify({"error": f"<div style='color: red;'>{str(e)}</div>"}), 500

# Goal 2
@app.route('/getWeatherStationDpot', methods=['GET'])
def get_weather_station_Dpot():
    try:
        logging.info("Received request for get_weather_station_Dpot")
        file_path = 'Processed/Available_dtapoint_demo.parquet'
        df = fetch_data_from_s3(buk, file_path)
        pandas_df = df.toPandas()
        html_content = pandas_df.to_html(classes='dataframe', index=False)

        return Response("""
        <html>
            <head>
                <title>Average Seasonal Temperature</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        margin: 20px;
                        background-color: #f3f3f3;
                    }}
                    h1 {{
                        text-align: center;
                        color: #333366;
                    }}
                    table {{
                        width: 100%;
                        border-collapse: collapse;
                        text-align: center;
                    }}
                    th, td {{
                        border: 1px solid #ccc;
                        padding: 8px;
                        text-align: center;
                    }}
                    th {{
                        background-color: #f2f2f2;
                    }}
                    tr:nth-child(even) {{
                        background-color: #f2f2f2;
                    }}
                </style>
            </head>
            <body>
                <h1>Average Seasonal Temperature</h1>
                <table id="myTable">
                    {}
                </table>
            </body>
        </html>
        """.format(html_content), content_type='text/html')

    except Exception as e:
        from flask import jsonify  # Make sure you import jsonify
        return jsonify({"error": f"<div style='color: red;'>{str(e)}</div>"}), 500

# Goal 3 - with 10 partitions
def get_average_temperature(lat1, lon1, lat2, lon2, startYear, endYear, df):
    from pyspark.sql import functions as F
    filtered_df = df.filter(
        (df.LATITUDE >= lat1) & (df.LATITUDE <= lat2) &
        (df.LONGITUDE >= lon1) & (df.LONGITUDE <= lon2) &
        (df.YEAR >= startYear) & (df.YEAR <= endYear)
    )

    avg_temp = filtered_df.agg(F.avg("TAVG").alias("Average Temperature")).collect()[0]["Average Temperature"]
    if avg_temp is not None:
        avg_temp = round(avg_temp,2)
    else:
        print("Average temperature is None. Setting to default value 0.")
        avg_temp = 'Not Found'  # Set to default integer value
    data_points = filtered_df.count()
    return {'Average Temperature': avg_temp, 'Data Points': data_points}

@app.route('/getCondAverageSeasonalTemperature', methods=['GET', 'POST'])
def get_cond_average_seasonal_temperature():
    try:
        if request.method == 'GET':
            return display_form()

        elif request.method == 'POST':
            file_path = 'Processed/Full_df_demo.parquet'
            df = fetch_data_from_s3(buk, file_path)  # Replace 'buk' with your bucket name

            lat1 = request.form.get('lat1', '0')
            lon1 = request.form.get('lon1', '0')
            lat2 = request.form.get('lat2', '0')
            lon2 = request.form.get('lon2', '0')
            startYear = request.form.get('startYear', '0')
            endYear = request.form.get('endYear', '0')

            result = get_average_temperature(
                float(lat1), float(lon1), float(lat2), float(lon2),
                int(startYear), int(endYear), df
            )
            return display_results(result, lat1, lon1, lat2, lon2, startYear, endYear)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
def display_form():
    return '''
    <html>
    <head>
        <title>Enter Coordinates for Average Temperature</title>
        <script type="text/javascript">
            function showProcessing() {
                var element = document.getElementById("processing");
                element.style.display = "block";
            }
        </script>
        <style>
            body {
                font-family: Arial, sans-serif;
                text-align: center;
            }
            .container {
                display: inline-block;
                text-align: left;
                padding: 20px;
                border: 1px solid #ccc;
                border-radius: 10px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                background-color: #CDD4DF; 
            }
            form div {
                margin-bottom: 15px;
            }
            #processing {
                display: none;
                font-size: 24px;
                color: #253F63;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <form method="post" onsubmit="showProcessing()">
                <div>Latitude 1: <input type="text" name="lat1"></div>
                <div>Latitude 2: <input type="text" name="lat2"></div>
                <div>Longitude 1: <input type="text" name="lon1"></div>
                <div>Longitude 2: <input type="text" name="lon2"></div>
                <div>Start Year: <input type="text" name="startYear"></div>
                <div>End Year: <input type="text" name="endYear"></div>
                <div><input type="submit" value="Submit"></div>
            </form>
        </div>
        <div id="processing">Processing...</div>
    </body>
    </html>
    '''
def display_results(result, lat1, lon1, lat2, lon2, startYear, endYear):
    return Response(f"""
            <html>
                <head>
                    <title>Average Temperature and Data Points for Coordinates </title>
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            margin: 20px;
                            background-color: #f3f3f3;
                        }}
                        h1 {{
                            text-align: center;
                            color: #333366;
                        }}
                        .result {{
                            background-color: #fff;
                            padding: 20px;
                            border-radius: 10px;
                            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                            text-align: center;
                        }}
                        .result h2, .result p {{
                            margin: 10px;
                            padding: 10px;
                        }}
                    </style>
                </head>
                <body>
                    <h1>Average Temperature and Data Points for Coordinates </h1>
                    <div class="result">
                        <h2>Average Temperature: {result['Average Temperature']}&deg;C</h2>
                        <p>(Lat: {lat1}, {lat2}; Lon: {lon1}, {lon2}) and Year Range ({startYear} - {endYear})
                        <p>Data Points: {result['Data Points']}</p>
                    </div>
                </body>
            </html>
            """, content_type='text/html')


# Welcome Page
@app.route('/', methods=['GET'])
def say_hello():
    return '''
    <html>
        <head>
            <title>Welcome Page</title>
            <style>
                /* Add CSS for center alignment and background */
                body {
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 0;
                    background-color: #628CA0;  /* Grey background */
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                }
                .container {
                    text-align: center;
                    background-color: #ffffff;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
                }
                h1 {
                    color: #333366;
                }
                p {
                    font-size: 18px;
                }
                a {
                    color: #336699;
                    text-decoration: none;
                }
                a:hover {
                    text-decoration: underline;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Hello, I am Kelsey!</h1>
                <p><a href="/getAverageSeasonalTemperature">Average Seasonal Temperature</a></p>
                <p><a href="/getWeatherStationDpot">List of weather stations and number of available datapoints</a></p>
                <p><a href="/getCondAverageSeasonalTemperature">Average Temperature and Data Points for Coordinates</a></p>
            </div>
        </body>
    </html>
    '''

# Run the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)





