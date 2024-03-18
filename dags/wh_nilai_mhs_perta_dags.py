import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models.param import Param, ParamsDict


import sys
sys.path.insert(0, '/opt/airflow')
from wh_script import NilaiMhsPerTA, FileTemporaryHandler

DB_CONNECTION_ID = "mysql_connection_psidevel"

with DAG(
    dag_id="wh_nilai_mhs_perta",
    params={
        "tahun_ajaran" : Param("20221", type="string", title="Tahun Ajaran"),
        "is_aktif" : Param(False, type="boolean", title="aktif")
    }
)as dag:
    script_nilai_mhs = NilaiMhsPerTA()
    file_temp_handler = FileTemporaryHandler()
    mysql_hook = MySqlHook(mysql_conn_id=DB_CONNECTION_ID)

    def get_data_nilai(**kwargs):
        params = kwargs.get("params")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        data_nilai = script_nilai_mhs.get_data_krs(cursor, params.get("is_aktif"), params.get("tahun_ajaran"))
        temp_file_path = file_temp_handler.generate_temporary_file('data_krs','csv')
        data_nilai.to_csv(temp_file_path, index=False)

        return temp_file_path

    def load_data_nilai(**kwargs):
        ti = kwargs['ti']
        temp_file = ti.xcom_pull(task_ids='get_data_nilai', key=None)

        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        script_nilai_mhs.synchronize_to_db(cursor, kwargs["params"].get("ta"), conn, temp_file)
    
    def update_database(**kwargs):
        pass
    def clean_up(**kwargs):
        pass

    get_nilai_task = PythonOperator(
        task_id='get_data_nilai',
        python_callable=get_data_nilai,
        provide_context=True,
    )
    load_data_task = PythonOperator(
        task_id='load_data_nilai',
        python_callable=load_data_nilai,
        provide_context=True,
    )
    update_database_task = PythonOperator(
        task_id='update_database',
        python_callable=update_database,
        provide_context=True,
    )
    clean_up_task = PythonOperator(
        task_id='clean_up',
        python_callable=clean_up,
        provide_context=True,
    )
