import json
import yaml
import boto3
import time
import os


session = boto3.Session(region_name="us-east-1") 
forecast = session.client(service_name='forecast')

def lambda_handler(event, context):

    predictor_arn = event["predictor_arn"]
    ## Unique identifier for the Predictor
    forecast_identifier = event["identifier"]
    predictor_type = event["type"]
    
    print(forecast_identifier)
    
    forecast_name= "ab3_" + predictor_type + "_" + forecast_identifier + "_forecast"
    
    class CreatePendingException(Exception):
        pass

    status = check_forecast_status(forecast_name)
    print(status)
    if status == "ACTIVE":
        forecastArn = ""
        response = forecast.list_forecasts()
        for forecast_entry in response["Forecasts"]:
            if (forecast_name == forecast_entry["ForecastName"]):
                forecast_arn = forecast_entry['ForecastArn']
                return {
                    'statusCode': 200,
                    'body': 'Forecast successfully created or already exists',
                    'forecast' :
                    {
                        'forecast_arn': forecast_arn,
                        'identifier': forecast_identifier,
                        'type': predictor_type
                    }
                }
    elif status != "DOES_NOT_EXIST":
        raise CreatePendingException('Create Pending state')

    
    bucketname = 'forecast-ab3' 
    file_path = "/tmp/config.yaml"
    s3 = boto3.resource('s3')
    s3.Bucket(bucketname).download_file("config.yaml", file_path)

    with open(file_path, 'r') as stream:
        doc= yaml.safe_load(stream)
        
    root_element = doc[predictor_type]
    forecast_element = root_element.get("Forecast")
    if forecast_element == None:
        quantiles = ['0.10', '0.50', '0.90']
    else:
        quantiles = forecast_element["ForecastTypes"]
        if quantiles == None:
            quantiles = ['0.10', '0.50', '0.90']

    ## Generate Forecast 
    print("Creating Forecast ...")
    forecast_arn = generate_forecast(forecast_name, predictor_arn, quantiles)
    
    ## Give it 3 seconds before checking forecast status 
    time.sleep(3)

    status = check_forecast_status(forecast_name)
    if status != "ACTIVE":
        raise CreatePendingException('Create Pending state')

    return {
        'statusCode': 200,
        'message': 'Forecast generation complete',
        'forecast' :
        {
            'forecast_arn': forecast_arn,
            'identifier': forecast_identifier
        }
        
    }


def check_forecast_status(forecast_name):
    status = "DOES_NOT_EXIST"
    response = forecast.list_forecasts()

    for forecast_entry in response["Forecasts"]:
        if (forecast_name == forecast_entry["ForecastName"]):
            forecast_response = forecast.describe_forecast(ForecastArn=forecast_entry["ForecastArn"])
            status = forecast_response["Status"]
            return status
            
    return status


def generate_forecast(forecast_name, predictor_arn, quantiles):
    
    print("Generating forecast " + forecast_name)
    response=forecast.create_forecast(ForecastName=forecast_name, 
                                PredictorArn= predictor_arn,
                                ForecastTypes=quantiles)
    forecast_arn=response['ForecastArn']
    return forecast_arn

