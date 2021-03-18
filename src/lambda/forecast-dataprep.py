
import sys
import os
import json
import time
import datetime
import boto3
import pandas as pd
from pyathena import connect
import zipfile
import re
from io import BytesIO


def lambda_handler(event, context):
    bucket_details=event
    
    #bucket_details="s3://forecast-ab3/raw-data-ab3/rawdata.zip"
    print (bucket_details)

    ## Extract bucket name, folder and zip file name from bucket URL
    regex=re.compile('//')
    extract_without_s3=regex.split(bucket_details)
    regex=re.compile('/+')
    bucket_split=regex.split(extract_without_s3[1])
    
    prefix=""
    file_path=""
    for i in range(len(bucket_split)):
        if i==0:
            bucket=bucket_split[i]
        else:
            if i < len(bucket_split) -1:
                prefix=prefix + bucket_split[i] + "/"
            else:
                file_name=bucket_split[i]

    regex=re.compile('.zip')
    identifier= "from_" + regex.split(file_name)[0] 
    
    # Extract zip file contents
    extract_zip(bucket, prefix, file_name)

    # Data Prep using Athena
    
    ## Data Prep for TTS
    ## Last month for training the data 
    end_date='201706'
    tts_file_path = create_tts_data(bucket,identifier,end_date)
    
    ## Data Prep for RTS
    #create_related_data()
    rts_file_path = create_related_data_full(bucket,identifier)

    return {
        'statusCode': 200,
        'message': "Data prep complete",
        'dataset' :
        {
            'tts_location': tts_file_path,
            'rts_location': rts_file_path,
            'identifier': identifier
        }
    }

def extract_zip(bucket, prefix, file_name):
    s3 = boto3.client('s3', use_ssl=False)
    Key_unzip = '/'

    zipped_keys =  s3.list_objects_v2(Bucket=bucket, Prefix=prefix + file_name, Delimiter = "/")
    file_list = []
    for key in zipped_keys['Contents']:
        file_list.append(key['Key'])
    #This will give you list of files in the folder you mentioned as prefix
    s3_resource = boto3.resource('s3')
    #Now create zip object one by one, this below is for 1st file in file_list
    zip_obj = s3_resource.Object(bucket_name=bucket, key=file_list[0])
    print (zip_obj)
    buffer = BytesIO(zip_obj.get()["Body"].read())
    
    z = zipfile.ZipFile(buffer)
    for filename in z.namelist():
        file_info = z.getinfo(filename)
        s3_resource.meta.client.upload_fileobj(
            z.open(filename),
            Bucket=bucket,
            Key=prefix + f'{filename}')
            

def create_tts_data(bucket_name,identifier,end_date):
    conn = connect(s3_staging_dir='s3://forecast-ab3/athena',
               region_name='us-east-1')

    target = pd.read_sql("Select sku as item_id, CONCAT(SUBSTR(yearmonth,1,4), '-', SUBSTR(yearmonth,5,6), '-01') as date," + 
                            "volume as demand, agency as location from ab3.historical_volume Where yearmonth < '" + end_date + "' " +
                            "Order by yearmonth, sku, agency;", conn)
    print (target)
    
    key="target-ts-" + identifier + ".csv"
    region= "us-east-1"
    
    target.to_csv("/tmp/"+key, header=False, index=False)
    boto3.Session(region_name=region).resource('s3').Bucket(bucket_name).upload_file("/tmp/"+key, "input-data/"+key)
    tts_file_path = "s3://" + bucket_name + "/input-data/"+ key
    return tts_file_path


def create_related_data(bucket_name, identifier):
    conn = connect(s3_staging_dir='s3://forecast-ab3/athena',
               region_name='us-east-1')

    related = pd.read_sql("Select sku as item_id, CONCAT(SUBSTR(yearmonth,1,4), '-', SUBSTR(yearmonth,5,6), '-01') as date, price," + 
                            "promotions, agency as location from ab3.price_promotions " + 
                            "Order by yearmonth,sku,agency;" , conn)
    print (related)
    
    key="related-ts-" + identifier + ".csv" 
    region= "us-east-1"
    
    related.to_csv("/tmp/"+key, header=False, index=False)
    boto3.Session(region_name=region).resource('s3').Bucket(bucket_name).upload_file("/tmp/"+key, "input-data/"+key)
    rts_file_path = "s3://" + bucket_name + "/input-data/"+ key
    return rts_file_path


def create_related_data_full(bucket_name, identifier):
    conn = connect(s3_staging_dir='s3://forecast-ab3/athena',
               region_name='us-east-1')

    related = pd.read_sql("Select A.sku as item_id, CONCAT(SUBSTR(A.yearmonth,1,4), '-', SUBSTR(A.yearmonth,5,6), '-01') as date," + 
                            "A.price as price,  A.promotions as promotions, A.agency as location," +
                            "(B.hol_1 + B.hol_2 + B.hol_3 + B.hol_4 + B.hol_5 + B.hol_6 + B.hol_7 + B.hol_8 " + 
                            "+ B.hol_9 + B.hol_10 + B.hol_11 + B.hol_12) as holiday_count, C.avg_max_temp as avg_max_temp " +
                            "from ab3.price_promotions A,ab3.holidays B, ab3.weather C " +
                            "Where A.yearmonth=B.yearmonth And A.yearmonth=C.yearmonth " +
                            "And A.agency = C.agency Order by A.yearmonth,A.sku,A.agency;" , conn)

    key="related-ts-" + identifier + ".csv" 
    region= "us-east-1"

    related.to_csv("/tmp/"+key, header=False, index=False)
    boto3.Session(region_name=region).resource('s3').Bucket(bucket_name).upload_file("/tmp/"+key, "input-data/"+key)
    rts_file_path = "s3://" + bucket_name + "/input-data/"+ key
    return rts_file_path

def create_actuals_data():
    conn = connect(s3_staging_dir='s3://forecast-ab3/athena',
               region_name='us-east-1')

    actuals = pd.read_sql("Select lower(sku) as item_id, CONCAT(SUBSTR(yearmonth,1,4), '-', SUBSTR(yearmonth,5,6), '-01') as date," + 
                            "lower(agency) as wholesaler, volume as actuals " + 
                            "from ab3.historical_volume where yearmonth >= '201706' Order By sku, yearmonth;" , conn)
    print (actuals)
    
    key="test.csv"
    region= "us-east-1"
    bucket_name= "forecast-ab3"
    
    actuals.to_csv("/tmp/"+key, header=False, index=False)
    boto3.Session(region_name=region).resource('s3').Bucket(bucket_name).upload_file("/tmp/"+key, "actuals/test.csv")

