import datetime
import pendulum
import os

from common import ToBigQuery as tbq

from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.branch import BaseBranchOperator


from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

# [Define Default Setting] ##################################################################
# kst = pendulum.timezone('Asia/Seoul')

# [Define DAG] ##################################################################
with DAG(
    dag_id = "D_Fr_Local_To_BigQuery_TaskGroup",
    start_date=pendulum.datetime(2024,5,5, tz='UTC'),
    schedule="@once",
    tags=["ODS", "DW"],
) as dag:
    taskStart=EmptyOperator(
        task_id="taskStart"
    )

    # [Define Task] ##################################################################
    # joinTotalTask = EmptyOperator(
    #     task_id='join',
    #     trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    # )

    branchTableTask=EmptyOperator(
        task_id="branchTableTask"
    )

    # [GCS Task] ##################################################################
    for pathList in tbq.pathList():
        with TaskGroup(f"UPLOAD_GCS_{pathList}") as uploadGCSGroup:
            dataToParquetTask = PythonOperator(
                task_id=f"Extract_{pathList}",
                python_callable=tbq.toParquet,
                op_kwargs={"index":pathList},
            )
            deleteToGCSTask = GCSDeleteObjectsOperator(
                task_id=f"Delete_{pathList}",
                bucket_name='ayb-bigquery-test-bucket',
                objects=tbq.returnGCSObjectList(pathList),
                prefix='list',
                gcp_conn_id='google_cloud_default',
            )
            uploadToGCSTask = LocalFilesystemToGCSOperator(
                task_id=f"Upload_{pathList}",
                src=[ f'/opt/airflow/datas/{pathList}/{fileNm}' for fileNm in list(os.listdir(f'/opt/airflow/datas/{pathList}/')) if fileNm.endswith('.parquet')],
                bucket='ayb-bigquery-test-bucket',
                dst=f'data/{pathList}/',
                gcp_conn_id='google_cloud_default',
            )
            dataToParquetTask >> deleteToGCSTask >> uploadToGCSTask


        # [BigQuery Task] ##################################################################
        with TaskGroup(f'INPUT_BIGQUERY_{pathList}') as inputBigQueryGroup:
            branchBigQueryTableExistsTask = BranchPythonOperator(
                task_id=f'branchBigQueryTableExistsTask_{pathList}',
                python_callable=tbq.checkBigQueryTableExists,
                op_kwargs={"tableName": pathList},
            )

            with TaskGroup(f'CREATE_BIGQUERY_TABLE_{pathList}') as createBigQueryTableGroup:
                createBigQueryExternalTableTask = EmptyOperator(
                    task_id=f'createBigQueryExternalTableTask_{pathList}',
                )
                createBigQueryManageTableTask = EmptyOperator(
                    task_id=f'createBigQueryManageTableTask_{pathList}',
                )
                createBigQueryExternalTableTask >> createBigQueryManageTableTask


            mergeBigQueryTableTask = EmptyOperator(
                task_id=f'mergeBigQueryTableTask_{pathList}',
            )

            branchBigQueryTableExistsTask >> createBigQueryExternalTableTask >> createBigQueryManageTableTask >> mergeBigQueryTableTask
            branchBigQueryTableExistsTask >> mergeBigQueryTableTask

        taskStart >> branchTableTask >> uploadGCSGroup >> inputBigQueryGroup




