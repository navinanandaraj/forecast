import json
import boto3
import time

session = boto3.Session(region_name="us-east-1") 
forecast = session.client(service_name='forecast')


def lambda_handler(event, context):

    forecast_arn = event["forecast_arn"]
    ## Unique identifier for Forecast
    export_identifier = event["identifier"]
    type = event["type"]
    
    export_job_name = "EXPORT_JOB_FORECAST" + "_" + type + "_" + export_identifier
    
    class CreatePendingException(Exception):
        pass

    status = check_export_job_status(export_job_name)
    print(status)
    if status == "ACTIVE":
        forecastArn = ""
        response = forecast.list_forecast_export_jobs()
        for export_job in response["ForecastExportJobs"]:
            if (export_job_name == export_job["ForecastExportJobName"]):
                export_job_arn = export_job['ForecastExportJobArn']
                return {
                    'statusCode': 200,
                    'message': 'Forecast successfully exported or export already exists',
                    'export_forecast' :
                    {
                        'export_forecast_arn': export_job_arn
                    }
                }
    elif status != "DOES_NOT_EXIST":
        raise CreatePendingException('Create Pending state')

    ## Exporting Forecast results    
    export_job_arn = export_forecast(export_job_name, forecast_arn, export_identifier)
    
    ## Give it 3 seconds before checking forecast status 
    time.sleep(3)

    status = check_export_job_status(export_job_name)
    if status != "ACTIVE":
        raise CreatePendingException('Create Pending state')

    return {
        'statusCode': 200,
        'message': 'Forecast exported successfully',
        'export_forecast' :
        {
            'export_forecast_arn': export_job_arn
        }
    }
    
def export_forecast(export_job_name, forecast_arn, export_identifier):

    role_name = "AB3-S3"
    role_arn = boto3.resource('iam').Role(role_name).arn

    print("Exporting forecast to S3 ...")
    response=forecast.create_forecast_export_job(ForecastExportJobName=export_job_name,
                                                          ForecastArn=forecast_arn,
                                                          Destination= {
                                                              "S3Config" : {
                                                                 "Path":"s3://forecast-ab3/output/"+export_identifier,
                                                                 "RoleArn": role_arn
                                                              } 
                                                          }
                                                )
    return response["ForecastExportJobArn"]


def check_export_job_status(export_job_name):
    status = "DOES_NOT_EXIST"
    response = forecast.list_forecast_export_jobs()

    for export in response["ForecastExportJobs"]:
        if (export_job_name == export["ForecastExportJobName"]):
            export_job_response = forecast.describe_forecast_export_job(ForecastExportJobArn=export["ForecastExportJobArn"])
            status = export_job_response["Status"]
            return status
            
    return status

