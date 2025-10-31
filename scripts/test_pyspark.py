from pyspark.sql import SparkSession

# 1️⃣ Create Spark session
spark = SparkSession.builder \
    .appName("PySparkTest") \
    .getOrCreate()

print("Spark session created successfully!")

# 2️⃣ Create a sample DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
print("DataFrame created successfully!")

# 3️⃣ Perform a simple transformation: filter Age > 28
df_filtered = df.filter(df.Age > 28)

# 4️⃣ Show the result
print("Filtered DataFrame (Age > 28):")
df_filtered.show()

# 5️⃣ Stop the Spark session
spark.stop()
print("Spark session stopped successfully!")
