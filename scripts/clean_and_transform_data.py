import time
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               DoubleType, StringType)
from pyspark.sql.functions import (col, to_timestamp, year, month, when, lit)

def clean_and_save_parquet(input_dir, output_dir):
    """
    Reads all CSVs from HDFS, cleans them, and saves as Parquet.
    """
    print(f"\nStarting data cleaning and Parquet conversion from {input_dir}")

    # Initialize Spark session
    spark = (SparkSession.builder
        .appName("NYC_Taxi_Cleaning")
        .config("spark.eventLog.enabled", "false")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate())

    # Define a schema aligned with the provided column descriptions
    schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("Passenger_count", IntegerType(), True),
        StructField("Trip_distance", DoubleType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("RateCodeID", IntegerType(), True),
        StructField("Store_and_fwd_flag", StringType(), True),
        StructField("Payment_type", IntegerType(), True),
        StructField("Fare_amount", DoubleType(), True),
        StructField("Extra", DoubleType(), True),
        StructField("MTA_tax", DoubleType(), True),
        StructField("Improvement_surcharge", DoubleType(), True),
        StructField("Tip_amount", DoubleType(), True),
        StructField("Tolls_amount", DoubleType(), True),
        StructField("Total_amount", DoubleType(), True)
    ])

    read_opts = {
        "header": "true",
        "mode": "PERMISSIVE",
        # read rows even if some columns malformed; we'll filter later
        "timestampFormat": "yyyy-MM-dd HH:mm:ss"
    }

    # Read all CSVs from HDFS
    df = spark.read.schema(schema).options(**read_opts).csv(
        f"hdfs://namenode:9000{input_dir}/*.csv")
    print(f"Loaded {df.limit(5).show()} rows from {input_dir}")

    # Convert timestamp strings to actual timestamps (try common formats)
    # We attempt two formats: "yyyy-MM-dd HH:mm:ss" and "MM/dd/yyyy
    # HH:mm:ss" fallback
    df = df.withColumn(
        "pickup_ts",
        to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "pickup_ts",
        when(col("pickup_ts").isNull(),
             to_timestamp(col("tpep_pickup_datetime"), "MM/dd/yyyy HH:mm:ss")
             ).otherwise(col("pickup_ts"))
    )

    df = df.withColumn(
        "dropoff_ts",
        to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "dropoff_ts",
        when(col("dropoff_ts").isNull(),
             to_timestamp(col("tpep_dropoff_datetime"), "MM/dd/yyyy HH:mm:ss")
             ).otherwise(col("dropoff_ts"))
    )

    # Cleaning rules (reasonable and documented):
    # - Drop rows where essential fields are missing (pickup_ts, total_amount)
    # - Ensure numeric fields are in plausible ranges:
    #     Trip_distance > 0 and <= 500 (arbitrary upper bound to remove outliers),
    #     Passenger_count between 1 and 8,
    #     Total_amount >= 0
    # - If Tip/Fare/Extra/Tolls are null, set to 0 for aggregation
    # Cleaning rules
    cleaned = df.filter(col("pickup_ts").isNotNull()) \
        .filter(col("Total_amount").isNotNull()) \
        .withColumn("Trip_distance",
                    when(col("Trip_distance").isNull(), lit(-1)).otherwise(
                        col("Trip_distance"))) \
        .withColumn("Passenger_count",
                    when(col("Passenger_count").isNull(), lit(-1)).otherwise(
                        col("Passenger_count"))) \
        .filter((col("Trip_distance") > 0) & (col("Trip_distance") <= 500)) \
        .filter((col("Passenger_count") >= 1) & (col("Passenger_count") <= 8)) \
        .filter(col("Total_amount") >= 0)

    # Fill monetary nulls
    monetary_cols = ["Fare_amount", "Extra", "MTA_tax",
                     "Improvement_surcharge", "Tip_amount", "Tolls_amount"]
    for c in monetary_cols:
        cleaned = cleaned.withColumn(c,
                                     when(col(c).isNull(), lit(0.0)).otherwise(
                                         col(c)))

    # Add year/month
    cleaned = cleaned.withColumn("year", year(col("pickup_ts"))) \
        .withColumn("month", month(col("pickup_ts")))

    cleaned.printSchema()
    print("Data cleaned. Writing Parquet files...")

    # Save cleaned data as Parquet
    cleaned.repartition("year", "month").write.mode("overwrite").parquet(
        f"hdfs://namenode:9000{output_dir}")
    print(f"Cleaned data saved to Parquet at {output_dir}")
    spark.stop()

if __name__ =='__main__':
    start = time.time()
    hdfs_dir = "/user/data"
    clean_output_dir = "/user/clean_data"
    clean_and_save_parquet(hdfs_dir,clean_output_dir)
    end = time.time()

    print(f"\nAll CSV files cleaned and transformed successfully in"
          f" {end - start} seconds")