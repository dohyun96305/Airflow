from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
from minio import Minio
from io import BytesIO

import requests
import json

BUCKET_NAME = 'stock-market'

def get_minio_conn() :
    minio = BaseHook.get_connection('minio')                    # Fetch the connection from the Airflow Meta DB (Admin - Connections)
    print(minio)
    client = Minio(                                             # Fetch the connection from the Minio, Based on Airflow Meta DB
        endpoint = minio.extra_dejson['endpoint_url'].split('//')[1], 
        access_key = minio.login,
        secret_key = minio.password,
        secure = False
    )

    return client

def _get_stock_prices(url : str, symbol : str) : 
    '''
    url -> str, url return by is_api_available task

    symbol -> str, Specific company want to fetch stock prices
    '''

    url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')                              # Fetch the connection from the Airflow Meta DB (Admin - Connections)
    response = requests.get(url, headers = api.extra_dejson['headers'])     # Requests to URL 

    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock : json) :
    '''
    stock -> json, stock file return by _get_stock_prices task
    '''
    client = get_minio_conn()                                       # Fetch the Minio onnection from the Airflow Meta DB (Admin - Connections)

    if not client.bucket_exists(BUCKET_NAME) :                      # Bucket Check, if not exists -> then create bucket with bucket_name
        client.make_bucket(BUCKET_NAME)

    stock = json.loads(stock)                                       # Converts Json type Str stock file to Python Dictionaries
    symbol = stock['meta']['symbol']                                # Name of the stock, Data stock => From _get_stock_prices json file, 
    data = json.dumps(stock, ensure_ascii = False).encode('utf8')   # Extract Data from json file stock, encoding with UTF-8

    objw = client.put_object(                                       # Store data to Minio Bucket (Name : bucket_name)
        bucket_name = BUCKET_NAME,
        object_name = f'{symbol}/prices.json',
        data = BytesIO(data),                                       # Data is come from Memory, So need to read data with BytesIO
        length = len(data)
    )

    return f'{objw.bucket_name}/{symbol}'                           # Return of File path (Inside the Minio Bucket-Symbol)

def _get_formatted_csv(path) : 
    '''
    path -> str, path where Data CSV file in Minio Storage
    '''
    client = get_minio_conn()                                                           # Fetch the Minio connection from the Airflow Meta DB (Admin - Connections)
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"                             # Path of Minio Data Storage
    objects = client.list_objects(BUCKET_NAME, prefix = prefix_name, recursive = True)  # list the objects in the directory

    for obj in objects : 
        if obj.object_name.endswith('.csv') : 
            return obj.object_name
    
    raise AirflowNotFoundException('The CSV file does not exist')                       # Raise Error Message if error 