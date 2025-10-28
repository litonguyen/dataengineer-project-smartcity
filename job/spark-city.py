from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from config import configuration

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming")\
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk:1.12.262")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .getOrCreate()
    

    spark.sparkContext.setLogLevel('WARN')

    vehicleSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('make', StringType(), True),
        StructField('model', StringType(), True),
        StructField('year', IntegerType(), True),
        StructField('fuelType', StringType(), True),
    ])

    gpsSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('vehicleType', StringType(), True),
    ])

    trafficSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('cameraId', StringType(), True),
        StructField('location', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('snapshot', StringType(), True),
    ])

    weatherSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('location', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('temperature', StringType(), True),
        StructField('weatherCondition', StringType(), True),
        StructField('precipitation', DoubleType(), True),
        StructField('windSpeed', DoubleType(), True),
        StructField('humidity', IntegerType(), True),
        StructField('airQualityIndex', DoubleType(), True),
    ])

    emergencySchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('incidentId', StringType(), True),
        StructField('type', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('status', StringType(), True),
        StructField('description', StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value as STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '5 minutes')
                )

    def stream_writer(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start()
                )

    vehicle_df = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gps_df = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    traffic_df = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weather_df = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergency_df = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    query1 = stream_writer(vehicle_df, 's3a://spark-streaming-data-lito/checkpoints/vehicle_data', 's3a://spark-streaming-data-lito/data/vehicle_data')
    query2 = stream_writer(gps_df, 's3a://spark-streaming-data-lito/checkpoints/gps_data', 's3a://spark-streaming-data-lito/data/gps_data')
    query3 = stream_writer(traffic_df, 's3a://spark-streaming-data-lito/checkpoints/traffic_data', 's3a://spark-streaming-data-lito/data/traffic_data')
    query4 = stream_writer(weather_df, 's3a://spark-streaming-data-lito/checkpoints/weather_data', 's3a://spark-streaming-data-lito/data/weather_data')
    query5 = stream_writer(emergency_df, 's3a://spark-streaming-data-lito/checkpoints/emergency_data', 's3a://spark-streaming-data-lito/data/emergency_data')

    spark.streams.awaitAnyTermination()
if __name__ == "__main__": 
    main()