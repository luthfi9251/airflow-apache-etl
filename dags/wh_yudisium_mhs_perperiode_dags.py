import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param, ParamsDict


import sys
sys.path.insert(0, '/opt/airflow')
from wh_script import YudisiumMhs, FileTemporaryHandler, get_connection_db


with DAG(
    dag_id="wh_yudisium_per_periode",
    params={
        "wisuda_ke" : Param(78, type="number", title="Wisuda Ke")
    }
) as dag:
    script_yudisium_mhs = YudisiumMhs()
    file_temp_handler = FileTemporaryHandler()
    mysql_hook = get_connection_db()

    def get_data_yudisium(**kwargs):
        params = kwargs.get("params")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        data_yudisium = script_yudisium_mhs.get_data_yudisium(cursor, str(params.get("wisuda_ke")))
        yudisium_file_path = file_temp_handler.generate_temporary_file('data_yudisium','csv')

        data_yudisium.to_csv(yudisium_file_path, index=False)

        return yudisium_file_path
    
    def load_data(**kwargs):
        ti = kwargs.get("ti")
        params = kwargs.get("params")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        yud_file_path = ti.xcom_pull(task_ids='get_data_yudisium', key=None)

        script_yudisium_mhs.load_data(cursor, conn, params.get("wisuda_ke"), yud_file_path)
        return "finish load data"
    
    def clean_up(**kwargs):
        ti = kwargs['ti']
        yud_file_path = ti.xcom_pull(task_ids='get_data_yudisium', key=None)
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        cursor.close()
        conn.close()
        file_temp_handler.clear_temp_folder([yud_file_path])
        return "finish clean up"
    
    get_data_task = PythonOperator(
        task_id='get_data_yudisium',
        python_callable=get_data_yudisium,
        provide_context=True,
    )
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )
    cleanup_data_task = PythonOperator(
        task_id='clean_up',
        python_callable=clean_up,
        provide_context=True,
    )

    get_data_task >> load_data_task >> cleanup_data_task