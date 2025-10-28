# NewYork_Taxi_Data_Analysis
The project uses NYC yellow cab data (2019–2020) to build a Hadoop-based system for storing and analyzing mobility patterns, routes, and ride volumes before and during COVID-19. It examines trip distance, duration, and passengers, highlighting key trends and changes in urban mobility.

dataset: https://www.kaggle.com/datasets/microize/newyork-yellow-taxi-trip-data-2020-2019?fbclid=IwZXh0bgNhZW0CMTAAYnJpZBExNEd0Z2k0MXY5bkZJNjdRdgEeMELaLJL7WmzuuShke0EPY-UH9-PgaE85NEdSsTpw6LMZOeprrNEdxt4QN5w_aem_RjLuatN2g3u0Tkmuh0KbwA


Run `conda env create -f environment.yml` to create conda env for the project.<br>
Run `conda activate data-pipeline-env` to activate the environment.<br>
Run `docker-compose up -d` to run docker containers with hadoop in detached mode. <br>
Run `python extract_load_data_pipeline.py` to load data into hadoop. <br>
Check file dir on docker: `docker exec -it namenode hdfs dfs -ls /user/data` <br>
