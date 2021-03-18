import json
import time
import pandas as pd
import boto3
import os
import yaml


session = boto3.Session(region_name='us-east-1') 
forecast = session.client(service_name='forecast') 

rts_import_arn = ""

def lambda_handler(event, context):

    print(event)
    tts_location = event["tts_location"]
    rts_location = event["rts_location"]
    ## Unique identifier for the dataset group
    dg_identifier = event["identifier"]
    
    datasetGroupName="ab3_dg_" + dg_identifier
    tts_import_name = datasetGroupName + "_tts_import"
    rts_import_name = datasetGroupName + "_rts_import"

    class CreatePendingException(Exception):
        pass

    status = check_import_job_status(tts_import_name, rts_import_name)
    print(status)
    if status == "ACTIVE":
        datasetGroupArn = ""
        response = forecast.list_dataset_groups()
        for dsgroup in response["DatasetGroups"]:
            if (datasetGroupName == dsgroup["DatasetGroupName"]):
                datasetGroupArn = dsgroup['DatasetGroupArn']

        return {
            'statusCode': 200,
            'body': 'Datasets successfully created or already exists',
            'dataset_group' :
            {
                'datasetGroupArn': datasetGroupArn,
                'identifier': dg_identifier
            }
        }
    elif status != "DOES_NOT_EXIST":
        raise CreatePendingException('Create Pending state')

    ## Create Dataset group
    datasetGroupArn= create_dataset_group(datasetGroupName)
    
    ## Create and Import TTS 
    tts_dataset_arn = create_import_dataset_tts(datasetGroupName, tts_import_name, tts_location)
    
    ## Create and Import RTS
    #rts_dataset_arn = create_import_dataset_rts()
    rts_dataset_arn = create_import_dataset_rts_full(datasetGroupName, rts_import_name, rts_location)

    ## Add TTS and RTS datasets to datasetgroup
    forecast.update_dataset_group(DatasetGroupArn=datasetGroupArn, DatasetArns=[tts_dataset_arn, rts_dataset_arn])
    
    ## Give it 3 seconds before checking status 
    time.sleep(3)
    
    status = check_import_job_status(tts_import_name, rts_import_name)
    
    if status != "ACTIVE":
        raise CreatePendingException('Create Pending state')
        
    return {
        'statusCode': 200,
        'body': 'Datasets successfully created',
        'dataset_group' :
        {
            'datasetGroupArn': datasetGroupArn,
            'identifier': dg_identifier
        }
    }

def check_import_job_status(tts_import_name, rts_import_name):

    status = "DOES_NOT_EXIST"
    response = forecast.list_dataset_import_jobs()

    for import_job in response["DatasetImportJobs"]:
        if (tts_import_name == import_job["DatasetImportJobName"]):
            tts_import_response = forecast.describe_dataset_import_job(DatasetImportJobArn=import_job["DatasetImportJobArn"])
            status = tts_import_response["Status"]
            if status != "ACTIVE":
                return status
            else:
                rts_response = forecast.list_dataset_import_jobs()
                for import_job in rts_response["DatasetImportJobs"]:  
                    if (rts_import_name == import_job["DatasetImportJobName"]):
                        rts_import_response = forecast.describe_dataset_import_job(DatasetImportJobArn=import_job["DatasetImportJobArn"])
                        status = rts_import_response["Status"]
                        return status
 
    return status

def create_dataset_group(datasetGroupName):
    response = forecast.list_dataset_groups()
    for dsgroup in response["DatasetGroups"]:
        if (datasetGroupName == dsgroup["DatasetGroupName"]):
            print("Datasetgroup " + datasetGroupName + " already exists")
            return dsgroup['DatasetGroupArn']

    create_dataset_group_response = forecast.create_dataset_group(DatasetGroupName=datasetGroupName,
                                                              Domain="RETAIL",
                                                             )
    datasetGroupArn = create_dataset_group_response['DatasetGroupArn']
    #forecast.describe_dataset_group(DatasetGroupArn=datasetGroupArn)
    print("Created Dataset group")
    return datasetGroupArn


def create_import_dataset_tts(datasetGroupName, datasetImportJobName, tts_location):
    dataset_name=datasetGroupName + "_tts"
    
    response = forecast.list_datasets()
    existingDataSets = response["Datasets"]
    
    for ds in existingDataSets:
        if (ds["DatasetName"] == dataset_name):
            print("Dataset " + dataset_name + " already exists")
            return ds["DatasetArn"]
        
    schema ={
        "Attributes":[
          {
             "AttributeName":"item_id",
             "AttributeType":"string"
          },
          {
             "AttributeName":"timestamp",
             "AttributeType":"timestamp"
          },
          {
             "AttributeName":"demand",
             "AttributeType":"float"
          },
          {
             "AttributeName":"location",
             "AttributeType":"string"
          }
        ]
    }

    response=forecast.create_dataset(
                    Domain="RETAIL",
                    DatasetType='TARGET_TIME_SERIES',
                    DatasetName=dataset_name,
                    DataFrequency="M",
                    Schema=schema)
    
    print("Created TTS Dataset")                
    datasetArn = response['DatasetArn']
    
    print("Dataset ARN " + datasetArn)
    
    role_name = "AB3-S3"
    role_arn = get_or_create_iam_role(role_name)
    
    ds_import_job_response=forecast.create_dataset_import_job(DatasetImportJobName=datasetImportJobName,
                                                          DatasetArn=datasetArn,
                                                          DataSource= {
                                                              "S3Config" : {
                                                                 "Path":tts_location,
                                                                 "RoleArn": role_arn
                                                              } 
                                                          },
                                                          TimestampFormat="yyyy-MM-dd"
                                                         )
    print("Imported TTS Dataset")
    return datasetArn


def create_import_dataset_rts_full(datasetGroupName, datasetImportJobName, rts_location):

    dataset_name=datasetGroupName + "_rts"
    response = forecast.list_datasets()
    existingDataSets = response["Datasets"]
    for ds in existingDataSets:
        if (ds["DatasetName"] == dataset_name):
            print("Dataset " + dataset_name + " already exists")
            return ds["DatasetArn"]
    
    ## Get RTS schema from params file
    bucketname = 'forecast-ab3' 
    file_path = "/tmp/params.yaml"
    s3 = boto3.resource('s3')
    s3.Bucket(bucketname).download_file("raw-data-ab3/params.yaml", file_path)
    with open(file_path, 'r') as stream:
        doc= yaml.safe_load(stream)
    params = doc["Params"]
    rts_schema = params.get("RTS_SCHEMA")
    attribute_list = rts_schema.get("Attributes")


    ## Construct dict RTS schema 
    schema = { "Attributes":[]}
    for i in range(0,len(attribute_list)):
        attribute = {}
        attribute["AttributeName"]=attribute_list[i].get("AttributeName")
        attribute["AttributeType"]=attribute_list[i].get("AttributeType")
        schema["Attributes"].append(attribute)

    print(schema)
    
    ## Create RTS dataset
    response=forecast.create_dataset(
                    Domain="RETAIL",
                    DatasetType='RELATED_TIME_SERIES',
                    DatasetName=dataset_name,
                    DataFrequency="M",
                    Schema=schema)
    
    print("Created RTS Dataset")                
    datasetArn = response['DatasetArn']
    
    print("Dataset ARN " + datasetArn)
    
    role_name = "AB3-S3"
    role_arn = get_or_create_iam_role(role_name)
    
    ds_import_job_response=forecast.create_dataset_import_job(DatasetImportJobName=datasetImportJobName,
                                                          DatasetArn=datasetArn,
                                                          DataSource= {
                                                              "S3Config" : {
                                                                 "Path":rts_location,
                                                                 "RoleArn": role_arn
                                                              } 
                                                          },
                                                          TimestampFormat="yyyy-MM-dd"
                                                         )
                                                         
    print("Imported RTS Dataset")
    return datasetArn



def get_or_create_iam_role( role_name ):
    iam = boto3.client("iam")

    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
              "Effect": "Allow",
              "Principal": {
                "Service": "forecast.amazonaws.com"
              },
              "Action": "sts:AssumeRole"
            }
        ]
    }

    try:
        create_role_response = iam.create_role(
            RoleName = role_name,
            AssumeRolePolicyDocument = json.dumps(assume_role_policy_document)
        )
        role_arn = create_role_response["Role"]["Arn"]
        print("Created", role_arn)
    except iam.exceptions.EntityAlreadyExistsException:
        print("The role " + role_name + " exists, ignore to create it")
        role_arn = boto3.resource('iam').Role(role_name).arn
        return role_arn

    print("Attaching policies")

    iam.attach_role_policy(
        RoleName = role_name,
        PolicyArn = "arn:aws:iam::aws:policy/AmazonForecastFullAccess"
    )

    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn='arn:aws:iam::269511639055:policy/AB3-forecast-s3',
    )

    print("Waiting for a minute to allow IAM role policy attachment to propagate")
    time.sleep(60)

    print("Done.")
    return role_arn
