import os
import shutil
from zipfile import ZipFile
import kagglehub
import subprocess
import csv
import io


def load_chunk_to_hdfs(rows_buffer, header, chunk_idx, hdfs_dir, file_name):
    process = subprocess.Popen(
        ["docker", "exec", "-i", "namenode", "hdfs", "dfs",
         "-appendToFile", "-", f"{hdfs_dir}/{file_name}"],
        stdin=subprocess.PIPE
    )

    with (io.TextIOWrapper(process.stdin,
                           encoding='latin1',
                           newline='')
          as text_stream):
        writer = csv.writer(text_stream,
                            quoting=csv.QUOTE_MINIMAL)
        if chunk_idx == 0:
            writer.writerow(header)
        writer.writerows(rows_buffer)

    process.stdin.close()
    process.wait()


if __name__ =='__main__':

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

        # Extract CSV if needed
        if zip_path.endswith(".zip"):
            with ZipFile(zip_path, 'r') as z:
                z.extractall("data/")
            csv_path = os.path.join("data", file_name)
        else:
            if os.path.exists(csv_path):
                os.remove(csv_path)
            shutil.move(zip_path, csv_path)

        # Chunk size for streaming
        chunk_size = 500_000
        rows_buffer = []
        chunk_idx = 0

        with open(csv_path, newline='', encoding='latin1') as fin:
            reader = csv.reader(fin, quotechar='"', delimiter=',', skipinitialspace=True)
            header = next(reader)

            for row in reader:
                rows_buffer.append(row)

                if len(rows_buffer) >= chunk_size:
                    print(f"Uploading chunk {chunk_idx} to HDFS...")

                    load_chunk_to_hdfs(rows_buffer,
                                       header,
                                       chunk_idx,
                                       hdfs_dir,
                                       file_name)
                    rows_buffer = []
                    chunk_idx += 1

            # Upload any remaining rows
            if rows_buffer:
                print(f"Uploading final chunk {chunk_idx} to HDFS...")
                load_chunk_to_hdfs(rows_buffer,
                                   header,
                                   chunk_idx,
                                   hdfs_dir,
                                   file_name)

        print(f"Finished {file_name} ({chunk_idx + 1} chunks uploaded)")

    print("\nAll CSV files uploaded to HDFS successfully!")
