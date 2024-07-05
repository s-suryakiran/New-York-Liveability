CREATE EXTERNAL TABLE airquality (
    borough STRING,
    name STRING,
    measureinfo  STRING,
    year INT,
    datavalue STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/user/pa2490_nyu_edu/livability/air_quality';



CREATE EXTERNAL TABLE shooting (
    zipcode STRING,
    burough STRING,
    year INT,
    latitude STRING,
longitude STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/user/pa2490_nyu_edu/livability/shooting';



CREATE EXTERNAL TABLE facilities  (
    zipcode STRING,
    burough STRING,
    facilityname STRING,
    facilitytype STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/user/pa2490_nyu_edu/livability/facility';


CREATE EXTERNAL TABLE complaint  (burough STRING,lawCatCd STRING,reportDate STRING,latitude STRING,longitude STRING,zipcode STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/user/pa2490_nyu_edu/livability/nypd_police_complaint';



CREATE EXTERNAL TABLE subway  (zipcode STRING,stopname STRING,burough STRING,latitude STRING,longitude STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/user/pa2490_nyu_edu/livability/subway/';


CREATE EXTERNAL TABLE shop (zipcode STRING,burough STRING,restaurantname STRING,yearawarded STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/user/pa2490_nyu_edu/livability/shops';



CREATE EXTERNAL TABLE bus_location (zipcode STRING,currentloc STRING,destinationloc STRING,originloc STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/user/pa2490_nyu_edu/livability/bus_location';


CREATE EXTERNAL TABLE bus_shelter (zipcode STRING,sheltercount INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/user/pa2490_nyu_edu/livability/bus_shelter';



CREATE EXTERNAL TABLE health_facilities (
    address STRING,
    zipcode STRING,
    countycode STRING,
    county STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '_'
STORED AS TEXTFILE
LOCATION '/user/pa2490_nyu_edu/livability/health_facility_data';



CREATE EXTERNAL TABLE citi_bikes (zipcode STRING,bike_type STRING,ride_count_per_bike_type_per_station INT,end_station_name STRING,end_station_id STRING,start_lat STRING,start_lng STRING,end_lat STRING,end_lng STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '_'
STORED AS TEXTFILE
LOCATION '/user/pa2490_nyu_edu/livability/citibike_data';



CREATE EXTERNAL TABLE restaurants (zipcode STRING,name STRING,burough STRING,latitude STRING,longitude STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/user/pa2490_nyu_edu/livability/restaurant';