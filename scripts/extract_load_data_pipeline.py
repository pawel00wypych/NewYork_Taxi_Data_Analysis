import os
import kagglehub
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

def process_file(files_path, file_name):
    thread_name = threading.current_thread().name
    csv_path = os.path.join(files_path, file_name)

    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        return

    print(
        f"\nUploading {file_name} ({os.path.getsize(csv_path) / (1024 * 1024):.1f} MB)"
        f" to HDFS using {thread_name}")

    hdfs_path = f"{hdfs_dir}/{file_name}"

    # Stream the whole file in one go (safe with 16GB RAM)
    with open(csv_path, "rb") as f:
        process = subprocess.Popen(
            ["docker", "exec", "-i", "namenode", "hdfs", "dfs", "-put",
             "-f", "-", hdfs_path],
            stdin=subprocess.PIPE
        )
        process.communicate(input=f.read())  # send file data

    if process.returncode == 0:
        print(f"Finished uploading {file_name} with {thread_name}")
    else:
        print(f"Upload failed for {file_name} (exit {process.returncode})")

if __name__ =='__main__':
    start = time.time()
    hdfs_dir = "/user/data"
    # csv_files = [
    #     "taxi%2B_zone_lookup.csv",
    #     "yellow_tripdata_2019-01.csv",
    #     "yellow_tripdata_2019-02.csv",
    #     "yellow_tripdata_2019-03.csv",
    #     "yellow_tripdata_2019-04.csv",
    #     "yellow_tripdata_2019-05.csv",
    #     "yellow_tripdata_2019-06.csv",
    #     "yellow_tripdata_2019-07.csv",
    #     "yellow_tripdata_2019-08.csv",
    #     "yellow_tripdata_2019-09.csv",
    #     "yellow_tripdata_2019-10.csv",
    #     "yellow_tripdata_2019-11.csv",
    #     "yellow_tripdata_2019-12.csv",
    #     "yellow_tripdata_2020-01.csv",
    #     "yellow_tripdata_2020-02.csv",
    #     "yellow_tripdata_2020-03.csv",
    #     "yellow_tripdata_2020-04.csv",
    #     "yellow_tripdata_2020-05.csv",
    #     "yellow_tripdata_2020-06.csv",
    # ]

    csv_files = [
        "taxi%2B_zone_lookup.csv",
        "yellow_tripdata_2019-01.csv",
        "yellow_tripdata_2019-02.csv",
        "yellow_tripdata_2019-03.csv",
        "yellow_tripdata_2019-04.csv",
        "yellow_tripdata_2019-05.csv"]

    # Download latest version
    csv_files_path = kagglehub.dataset_download(
        "microize/newyork-yellow-taxi-trip-data-2020-2019")
    print("Path to dataset files:", csv_files_path)

    print(f"Cleaning up old files from HDFS directory: {hdfs_dir}")
    subprocess.run(
        ["docker", "exec", "namenode", "hdfs", "dfs", "-rm", "-r", "-f",
         hdfs_dir],
        check=False
    )

    # Recreate the directory (so it always exists)
    subprocess.run(
        ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p",
         hdfs_dir],
        check=True
    )

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_file,
                                   csv_files_path,
                                   csv_file)
                   for csv_file in csv_files]
        for f in as_completed(futures):
            f.result()

    end = time.time()

    print(f"\nAll CSV files uploaded to HDFS successfully in {end - start} "
          f"seconds")
