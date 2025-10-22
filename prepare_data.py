import os
from zipfile import ZipFile
import pandas as pd
import kagglehub
from kagglehub import KaggleDatasetAdapter

# The CSV inside the Kaggle dataset
file_name = "yellow_tripdata_2020-01.csv"

# Download the dataset (this returns the local path to the ZIP file)
zip_path = kagglehub.dataset_download(
    "microize/newyork-yellow-taxi-trip-data-2020-2019",
    file_name
)

# Make folder to extract CSV
os.makedirs("data", exist_ok=True)

# Extract the CSV from the ZIP
with ZipFile(zip_path, 'r') as z:
    z.extractall("data/")  # CSV will now be in data/

# Read the CSV into a DataFrame
df = pd.read_csv(
    os.path.join("data", file_name),
    encoding="latin1",
    engine="python",
    on_bad_lines="skip"
)

print("First 5 records:")
print(df.head())
