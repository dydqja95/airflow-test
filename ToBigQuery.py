import os
import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from airflow.models import Variable


columns = {
        "ID": "ID",
        "사고일시": "Accident_Date",
        "요일": "Day_of_Week",
        "기상상태": "Weather_Condition",
        "시군구": "Municipality",
        "도로형태": "Road_Type",
        "노면상태": "Surface_Condition",
        "사고유형": "Accident_Type",
        "사고유형 - 세부분류": "Accident_Subtype",
        "법규위반": "Traffic_Violation",
        "가해운전자 차종": "A_Vehicle_Type",
        "가해운전자 성별": "A_Gender",
        "가해운전자 연령": "A_Age",
        "가해운전자 상해정도": "A_Injury_Severity",
        "피해운전자 차종": "V_Vehicle_Type",
        "피해운전자 성별": "V_Gender",
        "피해운전자 연령": "V_Age",
        "피해운전자 상해정도": "V_Injury_Severity",
        "사망자수": "Fatalities",
        "중상자수": "Serious_Injuries",
        "경상자수": "Minor_Injuries",
        "부상자수": "Total_Injuries",
        "ECLO": "ECLO"
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

    keyPath='/opt/airflow/datas/oauth/balmy-nuance-318401-bf7c6b963e78.json'
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


