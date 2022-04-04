#### Order of Dag executions wil be;

   1.1st_Dag_download_zippedfile.py: This one downloads a zipped file called "dutch-enery.zip" in the '/opt/airflow" path
   
   2.unzip_download_dataset.py: Picks up that zipped file from '/opt/airflow' path and while in '/opt/airflow' path, it creates 
     folders of 'datasets/energy' and thereafer unzips contents here.
     
   3.dag_Dutchenergy_toGSC.py: picks up files from the unzipped folders of '/Electrictrical' and from '/Gas' and thereafter changes
     files from 'file.csv' to 'file.parquet' and then upload them to google cloud bucket storage.
     
   4.dag_spark_task.py: picks the 'files.parquet' does transforms on them and then saves them back to google could bucket storage.
     a script i called 'dutch_energy_sparkscript.py' which is saved in the 'tasks' folder will be executed.
