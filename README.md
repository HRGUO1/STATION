# Weather Analysis REST API

## STATION

#### Welcome to the Weather Analysis REST API project! 

### Data Sources

- **Global Monthly Summary Data**: [Download here](https://www.ncei.noaa.gov/data/gsom/archive/gsom-latest.tar.gz)
- **Station Data**: [Download here](https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt)
- **Documentation**: [Read here](https://www1.ncdc.noaa.gov/pub/data/cdo/documentation/gsom-gsoy_documentation.pdf)

### Features --via Flask App Rest API (Lat_app.py)

- **Average Seasonal Temperature**: Obtain the average temperature for each season and year where data is available.
- **Weather Station Data Points**: List of weather stations and the number of non-null temperature entries for each season and year.
- **Regional and Time-Specific Analysis**: Accepts 2 sets of coordinates (`lat1, lon1, lat2, lon2`) and 2 integers (`startYear and endYear`) as parameters to serve average temperature and available datapoints for a rectangular area and time range.

#### Bonus Features -- only via Databricks (Monthly_Summary Demo.py)

- **Country Identification**: Determine the country of each weather station.
- **Global Change Metric**: A heatmap visualization representing changes in global temperature.
- **Outlier Identification**: Custom methods to identify significant outliers in the data.

### How to Use

1. **GET /getAverageSeasonalTemperature**: To get the average seasonal temperature data.
2. **GET /getWeatherStationDataPoints**: To obtain data points from various weather stations.
3. **GET|POST /getRegionalAndTimeSpecificData**: To get averaged data based on specific geographical coordinates and time range.


👉 **Feel free to read and download files. I recommend reading the workflow design firstly to figure out the goal of each file. :)**


