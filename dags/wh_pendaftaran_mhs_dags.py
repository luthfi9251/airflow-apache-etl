import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models.param import Param, ParamsDict


import sys
sys.path.insert(0, '/opt/airflow')
from wh_script import PendaftaranMhsPerTA, FileTemporaryHandler

DB_CONNECTION_ID = "mysql_connection_psidevel"

with DAG(
    dag_id="wh_pendaftaran_mhs_perta",
    params={
        "limit_tahun" : Param("2022", type="string", title="Limit Tahun")
    }
) as dag:
    script_pendaftaran_mhs = PendaftaranMhsPerTA()
    file_temp_handler = FileTemporaryHandler()
    mysql_hook = MySqlHook(mysql_conn_id=DB_CONNECTION_ID)
    
    def get_data_mhs(**kwargs):
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        dfMhs = script_pendaftaran_mhs.get_data_mhs(kwargs.get("params").get("limit_tahun"), cursor)
        temp_file_path = file_temp_handler.generate_temporary_file('data_mahasiswa','csv')
        dfMhs.to_csv(temp_file_path, index=False)

        return temp_file_path

    def get_yudisium_data(**kwargs):
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        (dfYud, dfYud_archive) = script_pendaftaran_mhs.get_yudisium_data(cursor)

        dfYud_file_path = file_temp_handler.generate_temporary_file('data_yud','csv')
        dfYud_archive_file_path = file_temp_handler.generate_temporary_file('data_yud_arch','csv')

        dfYud.to_csv(dfYud_file_path, index=False)
        dfYud_archive.to_csv(dfYud_archive_file_path, index=False)

        return (dfYud_file_path, dfYud_archive_file_path)

    def transform_data(**kwargs):
        ti = kwargs['ti']
        file_mhs = ti.xcom_pull(task_ids='get_data_mhs', key=None)
        (file_mhs_yud, file_mhs_yud_archive) = ti.xcom_pull(task_ids='get_yudisium_data', key=None)

        path = {
            "dfMhs": file_mhs,
            "dfYud": file_mhs_yud,
            "dfYud_archive": file_mhs_yud_archive
        }

        transformed_data = script_pendaftaran_mhs.transform_data(path)
        transformed_data_file_path = file_temp_handler.generate_temporary_file('transformed_data','csv')

        transformed_data.to_csv(transformed_data_file_path, index=False)

        return transformed_data_file_path

    def get_data_wh_exist(**kwargs):
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        df_wh_exist = script_pendaftaran_mhs.get_data_wh_exist(cursor)
        df_wh_exist_file_path = file_temp_handler.generate_temporary_file('df_wh_exist','csv')
        df_wh_exist.to_csv(df_wh_exist_file_path, index=False)

        return df_wh_exist_file_path

    def compare_data(**kwargs):
        ti = kwargs['ti']
        file_mhs = ti.xcom_pull(task_ids='transform_data', key=None)
        file_mhs_wh = ti.xcom_pull(task_ids='get_data_wh_exist', key=None)

        path = {
            "dfMhs": file_mhs,
            "dfMhsWh": file_mhs_wh
        }

        compared_data = script_pendaftaran_mhs.compare_data(path)
        compared_data_file_path = file_temp_handler.generate_temporary_file('compared_data','csv')

        compared_data.to_csv(compared_data_file_path, index=False)
        return compared_data


    get_data_mhs_task = PythonOperator(
        task_id='get_data_mhs',
        python_callable=get_data_mhs,
        provide_context=True,
    )
    get_yudisium_data_task = PythonOperator(
        task_id='get_yudisium_data',
        python_callable=get_yudisium_data,
        provide_context=True,
    )
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )
    get_wh_data_task = PythonOperator(
        task_id='get_wh_data',
        python_callable=get_data_wh_exist,
        provide_context=True,
    )
    compare_data_task = PythonOperator(
        task_id='compare_data',
        python_callable=compare_data,
        provide_context=True,
    )

    [get_data_mhs_task, get_yudisium_data_task] >> transform_data_task
    [get_wh_data_task, transform_data_task] >> compare_data_task
