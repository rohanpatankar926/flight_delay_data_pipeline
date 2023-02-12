from pyspark.sql import SparkSession
import functools
import pymongo
import json
import pandas as pd
import os
import shutil
from s3_syncer import S3Sync
from dotenv import load_dotenv
load_dotenv()

class SparkJoin:
    def __init__(self,data_path:str,output_dir:str):
        self.spark = SparkSession.builder.appName("FlightDelayPrediction").getOrCreate()
        self.data_path=data_path
        self.output_dir=output_dir
        self.partition_size=1
        
    def read_data(self):
        try:
            dataframe=[]
            for file in os.listdir(self.data_path):
                self.df=self.spark.read.option("header", "true").csv(f"data/{file}")
                dataframe.append(self.df)
            self.concat_df=functools.reduce(lambda x, y: x.union(y),dataframe)
            return self.concat_df
        except Exception as e:
            raise e

    def save_df(self):
        try:
            if os.path.exists("flight_data_csv"):
                shutil.rmtree("flight_data_csv")
            self.data=self.read_data()
            self.coalesced_df=self.data.coalesce(self.partition_size)
            return self.coalesced_df.write.format("csv").option("header","true").save(self.output_dir)
        except Exception as e:
            raise e

class DataDumpS3:
    def __init__(self):
        self.data_file_path=os.path.join(os.getcwd(),"flight_data_csv")

    def convert_df_to_json(self,data):
        try:
            return list(json.loads(data.T.to_json()).values())
        except Exception as e:
            raise e

    def dump_data(self):
        try:
            data_list=[]
            for file in os.listdir(self.data_file_path):
                if file.endswith(".csv"):
                    file_name=os.path.join (self.data_file_path,os.path.basename(file))
                    print(file_name)
                    chunk_iter = pd.read_csv(file_name, chunksize=10000)
                    for chunk in chunk_iter:
                        chunk.drop(["Unnamed: 27"],axis=1,inplace=True)
                        chunk.reset_index(drop=True,inplace=True)
                        data_list.extend(self.convert_df_to_json(data=chunk))
            json_data_save=os.path.join(os.getcwd(),"json_data_flight")
            os.makedirs(json_data_save,exist_ok=True)
            json.dump(data_list, open(os.path.join(json_data_save,"flight_delay.json"),"w"))
            s3_uri="s3://flightdataspark/flight-delay-dataset"
            S3Sync().s3_csv_folder_sync(json_data_save,s3_uri)
            print(f"{file_name} dumped to s3 bucket successfully")
            return "Successfully uploaded data to the s3 bucket"
        except Exception as e:
            raise e


if __name__=="__main__":
    data_path=os.path.join(os.getcwd(),"data")
    output_dir=os.path.join(os.getcwd(),"flight_data_csv")
    spark_join=SparkJoin(data_path=data_path,output_dir=output_dir)
    spark_join.read_data()
    spark_join.save_df()
    print("Dumping data to S3")
    data_dump=DataDumpS3()
    data_dump.dump_data()
