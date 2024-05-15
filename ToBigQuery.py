import os
import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from airflow.models import Variable


columns = {
        'test':'test'
    }


def toParquet(index) -> None:

    data = pd.read_csv('/opt/airflow/datas/train.csv', names=columns)
    colType = { col : 'str' for col in data.columns}
    data = data.astype(colType)
    data.to_parquet(f'/opt/airflow/datas/{index}/train_{index}.parquet')


def pathList() -> list:
    pathList = [ path for path in os.listdir('/opt/airflow/datas') if path.isnumeric()]

    return pathList

def returnGCSObjectList(tableName: str) -> list:

    keyPath='key.json'
    credentials=service_account.Credentials.from_service_account_file(keyPath)
    client = storage.Client(credentials=credentials, project=credentials.project_id)
    bucket = client.bucket('ayb-bigquery-test-bucket')
    blobs  = bucket.list_blobs(prefix=f'data/{tableName}/')

    # objectList = [blob.name.split(f'data/{tableName}/')[-1] for blob in blobs]
    objectList = [blob.name for blob in blobs]

    return objectList

def checkBigQueryTableExists(tableName: str) -> str:
    keyPath='/opt/airflow/datas/oauth/balmy-nuance-318401-bf7c6b963e78.json'
    credentails=service_account.Credentials.from_service_account_file(keyPath)
    client = bigquery.Client(credentials=credentails, project=credentails.project_id)
    tableList = client.list_tables('balmy-nuance-318401.ods')
    if tableName in tableList:
        return f'INPUT_BIGQUERY_{tableName}.mergeBigQueryTableTask_{tableName}'
    else:
        return f'INPUT_BIGQUERY_{tableName}.CREATE_BIGQUERY_TABLE_{tableName}.createBigQueryExternalTableTask_{tableName}'

# def tableColumnType(tableName: str, external: bool = False):
#     # Managed
#     if not bool:
#
#     # External
#     else:
#
#     return


