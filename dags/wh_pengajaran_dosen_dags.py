import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param, ParamsDict


import sys
sys.path.insert(0, '/opt/airflow')
from wh_script import PengajaranDosenPerTA, FileTemporaryHandler, get_connection_db


with DAG(
    dag_id="wh_pengajaran_dosen_perta",
    params={
        "tahun_ajaran" : Param("20221", type="string", title="Tahun Ajaran"),
        "is_aktif" : Param(False, type="boolean", title="aktif")
    }
)as dag:
    script_pengajaran_dosen = PengajaranDosenPerTA()
    file_temp_handler = FileTemporaryHandler()
    mysql_hook = get_connection_db()


    def get_data_dosen(**kwargs):
        params = kwargs.get("params")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        data_dosen = script_pengajaran_dosen.get_data_dosen_perTa(params.get("is_aktif"), params.get("tahun_ajaran"), cursor)
        temp_file_path = file_temp_handler.generate_temporary_file('data_dosen','csv')
        data_dosen.to_csv(temp_file_path, index=False)

        return temp_file_path
    
    def load_data_dosen(**kwargs):
        ti = kwargs['ti']
        temp_file = ti.xcom_pull(task_ids='extract_data_dosen', key=None)
        params = kwargs.get("params")

        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        script_pengajaran_dosen.insert_and_update_dosen_perTa(params.get("tahun_ajaran"), cursor, temp_file, conn)

    
    def clean_up(**kwargs):
        ti = kwargs['ti']
        temp_file = ti.xcom_pull(task_ids='extract_data_dosen', key=None)
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        cursor.close()
        conn.close()
        file_temp_handler.clear_temp_folder([temp_file])


    get_data_task = PythonOperator(
        task_id='extract_data_dosen',
        python_callable=get_data_dosen,
        provide_context=True,
    )
    load_data_task = PythonOperator(
        task_id='load_data_dosen',
        python_callable=load_data_dosen,
        provide_context=True,
    )
    clean_operation_task = PythonOperator(
        task_id='clean_up',
        python_callable=clean_up,
        provide_context=True,
    )

    get_data_task >> load_data_task >> clean_operation_task

