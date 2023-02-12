from flask import Flask,jsonify
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
import boto3
s3=boto3.client("s3")
app=Flask(__name__)

@app.route("/")
def index():
    return jsonify({"message":"This is the data api for flight delay prediction"})

def get_data_from_s3(bucket_name, key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        data = response['Body'].read().decode("utf-8)
        return data
    except Exception as e:
        raise e

@app.route('/data')
def get_data():
    data = get_data_from_s3(bucket_name='flightdataspark', key='flight-delay-dataset/flight_delay.json')
    return jsonify(data)


if __name__=="__main__":
    app.run(debug=True,port=8000,host="0.0.0.0")
