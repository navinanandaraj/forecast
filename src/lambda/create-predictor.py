import json
import yaml
import boto3
import time
import os


algo = "arn:aws:forecast:::algorithm/AutoML" # Default to Auto ML
forecast_horizon = "1" # Default to 1 month 
forecast_frequency = "M" # Default to Monthly
forecast_dimension = ""
backtest_windows= "1"
quantiles = ['0.10', '0.50', '0.90']

session = boto3.Session(region_name='us-east-1') 
forecast = session.client(service_name='forecast') 


def lambda_handler(event, context):

    datasetGroupArn = event["datasetGroupArn"]
    ## Unique identifier for the Predictor
    predictor_identifier = event["identifier"]

    type= get_type()
    predictor_name= "ab3_" + type + "_" + predictor_identifier + "_predictor"
    #predictor_name = "DeepAr_Backtest_full_ab3_predictor"

    class CreatePendingException(Exception):
        pass

    status = check_predictor_status(predictor_name)
    print(status)
    if status == "ACTIVE":
        predictorArn = ""
        response = forecast.list_predictors()
        for predictor in response["Predictors"]:
            if (predictor_name == predictor["PredictorName"]):
                predictorArn = predictor['PredictorArn']
                return {
                    'statusCode': 200,
                    'body': 'Predictor successfully created or already exists',
                    'predictor' :
                    {
                        'predictor_arn': predictorArn,
                        'identifier': predictor_identifier,
                        'type': type
                    }
                }
    elif status != "DOES_NOT_EXIST":
        raise CreatePendingException('Create Pending state')

    print("Parsing predictor config")
    get_params(type)

    ## Create Predictor
    print("Creating Predictor")
    predictor_arn = create_predictor(type, predictor_name, datasetGroupArn)

    ## Give it 3 seconds before checking predictor status 
    time.sleep(3)

    status = check_predictor_status(predictor_name)
    if status != "ACTIVE":
        raise CreatePendingException('Create Pending state')

    return {
        'statusCode': 200,
        'message': 'Predictor created successfully and training complete',
        'predictor' :
        {
            'predictor_arn': predictor_arn,
            'identifier': predictor_identifier
        }
        
    }


def get_type():
    ## Get algo type from params file
    bucketname = 'forecast-ab3' 
    file_path = "/tmp/params.yaml"
    s3 = boto3.resource('s3')
    s3.Bucket(bucketname).download_file("raw-data-ab3/params.yaml", file_path)
    with open(file_path, 'r') as stream:
        doc= yaml.safe_load(stream)
    params = doc["Params"]
    algo_type = params.get("ExecutionType")
    return algo_type


def check_predictor_status(predictor_name):

    status = "DOES_NOT_EXIST"
    response = forecast.list_predictors()

    for predictor in response["Predictors"]:
        if (predictor_name == predictor["PredictorName"]):
            predictor_response = forecast.describe_predictor(PredictorArn=predictor["PredictorArn"])
            status = predictor_response["Status"]
            return status
            
    return status


def create_predictor(type, predictor_name, datasetGroupArn):
    global algo
    global forecast_horizon
    global forecast_frequency
    global forecast_dimension
    global backtest_windows
    global quantiles
    
    if algo == "arn:aws:forecast:::algorithm/AutoML":
        perform_automl = True
    else:
        perform_automl = False
        
    print("Using algo " + algo)
    if perform_automl == False:
        response=forecast.create_predictor(PredictorName=predictor_name, 
                                    AlgorithmArn=algo, 
                                    ForecastHorizon=forecast_horizon,
                                    PerformAutoML=perform_automl,
                                    InputDataConfig= {"DatasetGroupArn": datasetGroupArn},
                                    EvaluationParameters= {"NumberOfBacktestWindows": backtest_windows, "BackTestWindowOffset": forecast_horizon},
                                    FeaturizationConfig= {"ForecastFrequency": forecast_frequency, "ForecastDimensions": forecast_dimension},
                                    ForecastTypes=quantiles
                                    )
    else:
        response=forecast.create_predictor(PredictorName=predictor_name,
                                    ForecastHorizon=forecast_horizon,
                                    PerformAutoML=perform_automl,
                                    InputDataConfig= {"DatasetGroupArn": datasetGroupArn},
                                    EvaluationParameters= {"NumberOfBacktestWindows": backtest_windows, "BackTestWindowOffset": forecast_horizon},
                                    FeaturizationConfig= {"ForecastFrequency": forecast_frequency, "ForecastDimensions": forecast_dimension},
                                    ForecastTypes=quantiles
                                    )
        
                                
    predictor_arn=response['PredictorArn']
    return predictor_arn


def get_params(type):
    global algo
    global forecast_horizon
    global forecast_frequency
    global forecast_dimension
    global backtest_windows
    global quantiles
    
    bucketname = 'forecast-ab3' 
    file_path = "/tmp/config.yaml"
    s3 = boto3.resource('s3')
    s3.Bucket(bucketname).download_file("config.yaml", file_path)

    with open(file_path, 'r') as stream:
        doc= yaml.safe_load(stream)
        
    predictor_elements = doc[type]["Predictor"]
    parsed_algo = predictor_elements.get("AlgorithmArn")
    if parsed_algo == None:
        print("Defaulting to AutoML")
    else:
        algo = parsed_algo

    print("Algo : " + algo)
    parsed_horizon = predictor_elements.get("ForecastHorizon")
    if parsed_horizon == None:
        print("Defaulting to forecast horizon of 1 month")
    else:
        forecast_horizon = parsed_horizon

    parsed_frequency= predictor_elements["FeaturizationConfig"].get("ForecastFrequency")
    if parsed_frequency == None:
        print("Defaulting to forecast frequency of monthly")
    else:
        forecast_frequency = parsed_frequency
        
    parsed_dimension= predictor_elements["FeaturizationConfig"].get("ForecastDimensions")
    if parsed_dimension == None:
        print("No custom forecast dimension")
    else:
        forecast_dimension = parsed_dimension


    forecast_element = doc[type].get("Forecast")
    if forecast_element == None:
        print("No custom forecast types")
    else:
        parsed_quantiles = forecast_element["ForecastTypes"]
        if parsed_quantiles != None:
            quantiles = parsed_quantiles


    evaluation_element = predictor_elements.get("EvaluationParameters")
    if evaluation_element == None:
        print("No custom evaluation parameters")
    else:
        parsed_backtest = evaluation_element["NumberOfBacktestWindows"]
        if parsed_backtest != None:
            backtest_windows = parsed_backtest

