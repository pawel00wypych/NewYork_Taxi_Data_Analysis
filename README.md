# NewYork_Taxi_Data_Analysis
The project uses NYC yellow cab data (2019–2020) to build a Hadoop-based system for storing and analyzing mobility patterns, routes, and ride volumes before and during COVID-19. It examines trip distance, duration, and passengers, highlighting key trends and changes in urban mobility.

dataset: https://www.kaggle.com/datasets/microize/newyork-yellow-taxi-trip-data-2020-2019?fbclid=IwZXh0bgNhZW0CMTAAYnJpZBExNEd0Z2k0MXY5bkZJNjdRdgEeMELaLJL7WmzuuShke0EPY-UH9-PgaE85NEdSsTpw6LMZOeprrNEdxt4QN5w_aem_RjLuatN2g3u0Tkmuh0KbwA

requirements:
- conda
- docker


Run `conda env create -f environment.yml` to create conda env for the project.<br>
Run `conda activate data-pipeline-env` to activate the environment.<br>
Run `docker-compose build` to download dependencies and build docker volumes.
<br>
Run `docker-compose up -d` to build and run docker 
containers in detached mode<br>
Run `python scripts/extract_load_data_pipeline.py` to extract and load data 
into hadoop.<br>
Run `docker exec spark-client spark-submit /scripts/clean_and_transform_data.py`
to clean and transform .csv files into parquet files on hadoop.<br>
Run `docker exec spark-client spark-submit /scripts/analyze_taxi_data.py`
to perform data analysis. Results will be available in scripts/analysis/ 
folder.<br><br>


If you encounter issues with the scripts, please check whether the file was 
saved with a CRLF (Windows) line separator — it should be LF (Linux).

Example of the analysis:
- Comparison of amount of rides per number of passengers in taxi between 
  2019 and 2020:
  <img width="1000" height="600" alt="Image" src="https://github.com/user-attachments/assets/540d92bc-eef6-4661-9452-ccd7384f12a3" />
- Comparison of tip amount per number of passengers in taxi between 2019 and 2020:
  <img width="1000" height="600" alt="Image" src="https://github.com/user-attachments/assets/452efa96-1199-4a32-8987-9f57e5885719" />
- Comparison of average and median trip distance per ride between 2019 and 2020:
  <img width="800" height="500" alt="Image" src="https://github.com/user-attachments/assets/a664a8a7-b170-4e12-9200-a818d7cd0ef6" />

