import os
import shutil
from zipfile import ZipFile
import kagglehub
import subprocess
import csv

# Kaggle dataset handle
dataset_handle = "microize/newyork-yellow-taxi-trip-data-2020-2019"

# Folder to extract CSVs
os.makedirs("data", exist_ok=True)

# HDFS target directory
hdfs_dir = "/user/data"

# Make sure HDFS directory exists
subprocess.run(
    ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_dir],
    check=True
)

csv_files = [
    "taxi%2B_zone_lookup.csv",
    "yellow_tripdata_2019-01.csv",
    "yellow_tripdata_2019-02.csv",
    "yellow_tripdata_2019-03.csv",
    "yellow_tripdata_2019-04.csv",
    "yellow_tripdata_2019-05.csv",
    "yellow_tripdata_2019-06.csv",
    "yellow_tripdata_2019-07.csv",
    "yellow_tripdata_2019-08.csv",
    "yellow_tripdata_2019-09.csv",
    "yellow_tripdata_2019-10.csv",
    "yellow_tripdata_2019-11.csv",
    "yellow_tripdata_2019-12.csv",
    "yellow_tripdata_2020-01.csv",
    "yellow_tripdata_2020-02.csv",
    "yellow_tripdata_2020-03.csv",
    "yellow_tripdata_2020-04.csv",
    "yellow_tripdata_2020-05.csv",
    "yellow_tripdata_2020-06.csv",
]

# Process each CSV file
for file_name in csv_files:
    print(f"\nProcessing {file_name}...")

    # Download the CSV ZIP
    zip_path = kagglehub.dataset_download(dataset_handle, file_name)

    csv_path = os.path.join("data", file_name)
    # Extract CSV
    if zip_path.endswith(".zip"):
        with ZipFile(zip_path, 'r') as z:
            z.extractall("data/")

        csv_path = os.path.join("data", file_name)
    else:
        if os.path.exists(csv_path):
            os.remove(csv_path)
        shutil.move(zip_path, csv_path)

    # Read and upload in chunks
    chunk_size = 500_000

    output_dir = "data/"
    os.makedirs(output_dir, exist_ok=True)

    with open(csv_path,
              newline='',
              encoding='latin1') as fin:

        reader = csv.reader(fin,
                            quotechar='"',
                            delimiter=',',
                            skipinitialspace=True)
        header = next(reader)
        chunk_rows = []
        chunk_idx = 0

        for row in reader:
            chunk_rows.append(row)
            if len(chunk_rows) >= chunk_size:
                out_file = os.path.join(output_dir, f"temp_chunk_{chunk_idx}.csv")

                with open(out_file,
                          "w",
                          newline='',
                          encoding='latin1') as fout:

                    writer = csv.writer(fout,
                                        quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(header)
                    writer.writerows(chunk_rows)
                chunk_rows = []
                chunk_idx += 1

        # write remaining rows
        if chunk_rows:
            out_file = os.path.join(output_dir, f"chunk_{chunk_idx}.csv")
            with open(out_file,
                      "w",
                      newline='',
                      encoding='latin1') as fout:
                writer = csv.writer(fout, quoting=csv.QUOTE_MINIMAL)
                writer.writerow(header)
                writer.writerows(chunk_rows)

        print(f"Uploading chunk {chunk_idx} to HDFS...")

        subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-appendToFile",
             f"/data/{os.path.basename(out_file)}", f"{hdfs_dir}/{file_name}"],
            check=True
        )

        os.remove(out_file)

print("\nAll CSV files uploaded to HDFS successfully!")
