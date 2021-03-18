import json
import boto3
from datetime import datetime


def lambda_handler(event, context):
    # TODO implement

    ## Get bucket name and object key from S3 event
    for record in event['Records']:
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]

    session = boto3.Session(region_name='us-east-1') 
    step_functions = session.client(service_name='stepfunctions') 

    ## Step Function execution name
    execution_name = "execution_" + datetime.now().strftime("%Y%m%d%H%M%S")
    ## Construct bucket details 
    bucket_details = "s3://" + bucket_name + "/" + object_key
    print(bucket_details)
    
    ## Invoke Step function to start forecast generation process
    step_functions.start_execution(stateMachineArn='arn:aws:states:us-east-1:269511639055:stateMachine:Forecast_AB3',
        name=execution_name,
        input="{\"bucket_details\":\"" + bucket_details +"\"}"
        )

    return {
        'statusCode': 200
    }

