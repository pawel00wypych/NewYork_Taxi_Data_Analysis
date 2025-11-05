import time
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import seaborn as sns

def analyze_taxi_data(input_dir):
    print(f"\nStarting analysis from cleaned Parquet data at {input_dir}")

    spark = (SparkSession.builder
             .appName("NYC_Taxi_Analysis")
             .config("spark.eventLog.enabled", "false")
             .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
             .getOrCreate())

    # Load cleaned data
    df = spark.read.parquet(f"hdfs://namenode:9000{input_dir}")

    # Filter only months Feb–Jun and years 2019–2020
    df = df.filter((col("month").between(2, 6)) & (col("year").isin(2019, 2020)))
    df.createOrReplaceTempView("trips")


    # count trips per year, month, and payment type
    payment_changes = spark.sql("""
                                SELECT year, month, Payment_type, COUNT (*) AS trip_count
                                FROM trips
                                GROUP BY year, month, Payment_type
                                ORDER BY year, month, Payment_type
                                """)

    pdf1 = payment_changes.toPandas()

    # Map numeric codes to human-readable names
    payment_labels = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }
    pdf1["Payment_desc"] = pdf1["Payment_type"].map(payment_labels)

    # Combine year and month into one column for the x-axis
    pdf1["YearMonth"] = pdf1["year"].astype(str) + "-" + pdf1["month"].astype(
        str).str.zfill(2)


    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=pdf1,
        x="YearMonth",
        y="trip_count",
        hue="Payment_desc"
    )

    plt.title("Payment Type Distribution by Month and Year", fontsize=14)
    plt.xlabel("Month-Year", fontsize=12)
    plt.ylabel("Trip Count", fontsize=12)
    plt.legend(title="Payment Type", bbox_to_anchor=(1.05, 1),
               loc="upper left")
    plt.grid(True, axis="y", alpha=0.6)
    plt.tight_layout()
    plt.savefig("/scripts/analysis/output_payment_type.png")
    plt.close()

    print("\nTip & Total amount by passenger count")

    tip_total = spark.sql("""
                          SELECT year, month, Passenger_count, ROUND(AVG (Tip_amount), 2) AS avg_tip, ROUND(AVG (Total_amount), 2) AS avg_total
                          FROM trips
                          GROUP BY year, month, Passenger_count
                          ORDER BY year, month, Passenger_count
                          """)

    pdf2 = tip_total.toPandas()

    # Combine year & month for the x-axis
    pdf2["YearMonth"] = pdf2["year"].astype(str) + "-" + pdf2["month"].astype(
        str).str.zfill(2)

    # --- Average Tip ---
    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=pdf2,
        x="Passenger_count",
        y="avg_tip",
        hue="year"
    )
    plt.title("Average Tip Amount by Passenger Count (2019 vs 2020)",
              fontsize=14)
    plt.xlabel("Passenger Count", fontsize=12)
    plt.ylabel("Average Tip ($)", fontsize=12)
    plt.legend(title="Year")
    plt.grid(True, axis="y", linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig("/scripts/analysis/output_tip_amount.png")
    plt.close()

    # --- Average Total ---
    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=pdf2,
        x="Passenger_count",
        y="avg_total",
        hue="year"
    )
    plt.title("Average Total Amount by Passenger Count (2019 vs 2020)",
              fontsize=14)
    plt.xlabel("Passenger Count", fontsize=12)
    plt.ylabel("Average Total ($)", fontsize=12)
    plt.legend(title="Year")
    plt.grid(True, axis="y", alpha=0.6)
    plt.tight_layout()
    plt.savefig("/scripts/analysis/output_total_amount.png")
    plt.close()

    # Number of rides per passenger count
    print("\nNumber of rides per Passenger_count")
    passenger_dist = spark.sql("""
        SELECT year, month, Passenger_count, COUNT(*) AS trip_count
        FROM trips
        GROUP BY year, month, Passenger_count
        ORDER BY year, month, Passenger_count
    """)
    pdf3 = passenger_dist.toPandas()

    plt.figure(figsize=(8,5))
    for yr in [2019, 2020]:
        subset = pdf3[pdf3["year"] == yr]
        plt.plot(subset["month"],
                 subset["trip_count"],
                 marker='o',
                 label=f"{yr}")
    plt.title("Number of rides per passenger count (02–06)")
    plt.xlabel("Month")
    plt.ylabel("Rides count")
    plt.legend()
    plt.grid(True)
    plt.savefig("/scripts/analysis/output_passenger_distribution.png")
    plt.close()

    print("\nNumber of rides per Passenger_count")
    passenger_dist = spark.sql("""
                               SELECT year, month, Passenger_count, COUNT (*) AS trip_count
                               FROM trips
                               GROUP BY year, month, Passenger_count
                               ORDER BY year, month, Passenger_count
                               """)

    pdf3 = passenger_dist.toPandas()

    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=pdf3,
        x="Passenger_count",
        y="trip_count",
        hue="year"
    )
    plt.title("Number of Rides by Passenger Count (2019 vs 2020)", fontsize=14)
    plt.xlabel("Passenger Count", fontsize=12)
    plt.ylabel("Trip Count", fontsize=12)
    plt.legend(title="Year")
    plt.grid(True, axis="y", alpha=0.6)
    plt.tight_layout()
    plt.savefig("/scripts/analysis/output_passenger_distribution.png")
    plt.close()

    # Avg & Median Trip Distance
    print("\nAvg & Median Trip Distance")
    dist_stats = spark.sql("""
        SELECT
            year,
            month,
            ROUND(AVG(Trip_distance), 2) AS avg_distance,
            ROUND(percentile_approx(Trip_distance, 0.5), 2) AS median_distance
        FROM trips
        GROUP BY year, month
        ORDER BY year, month
    """)
    pdf5 = dist_stats.toPandas()

    plt.figure(figsize=(8,5))
    for yr in [2019, 2020]:
        subset = pdf5[pdf5["year"] == yr]
        plt.plot(subset["month"],
                 subset["avg_distance"],
                 marker='o',
                 label=f"Avg {yr}")
        plt.plot(subset["month"],
                 subset["median_distance"],
                 marker='x',
                 linestyle='--',
                 label=f"Median {yr}")
    plt.title("Avg & Median Trip Distance (02–06)")
    plt.xlabel("Month")
    plt.ylabel("Distance (mile)")
    plt.legend()
    plt.grid(True)
    plt.savefig("/scripts/analysis/output_trip_distance.png")
    plt.close()

    print("\nResults saved in /scripts/analysis as PNG files:")
    print("   - output_payment_type.png")
    print("   - output_tip_amount.png")
    print("   - output_passenger_distribution.png")
    print("   - output_total_trips.png")
    print("   - output_trip_distance.png")

    spark.stop()


if __name__ == "__main__":
    start = time.time()
    subdir_path = "/scripts/analysis"
    if not os.path.exists(subdir_path):
        os.makedirs(subdir_path)
        print(f"Created directory: {subdir_path}")
    else:
        print(f"Directory already exists: {subdir_path}")

    analyze_taxi_data("/user/clean_data")
    end = time.time()
    print(f"\nAnalysis completed in {round(end - start, 2)} seconds.")
