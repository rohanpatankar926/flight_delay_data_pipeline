##### Follow these steps to run the docker container on local system
```
git clone https://github.com/rohanpatankar926/flight_delay_prediction.git

cd flight_delay/pipeline_rapid_api

sudo docker build -t spark_pipeline:v1 .

sudo docker run   --mount type=bind,source="$(pwd)",target=/opt/application/ pipeline_spark:v1   spark-submit --master local   /opt/application/data_pipeline.py

```

After running these things we can able to make combine data from batch data and then deploy the data to s3 bucket feature store for further processing of the data using ml lifecycle


#### For building docker image from dockerfile
sudo docker build -t pipeline_spark:v1 .

#### For running the docker spark application container 
sudo docker run   --mount type=bind,source="$(pwd)",target=/opt/application/ pipeline_spark:v1   spark-submit --master local   /opt/application/data_pipeline.py






