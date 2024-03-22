import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param, ParamsDict


import sys
sys.path.insert(0, '/opt/airflow')
from wh_script import KuliahMhsPerTa, FileTemporaryHandler, get_connection_db


with DAG(
    dag_id="wh_kuliah_mhs_perta",
    params={
        "tahun_ajaran" : Param("20221", type="string", title="Tahun Ajaran")
    }
)as dag:
    script_kuliah_mhs = KuliahMhsPerTa()
    file_temp_handler = FileTemporaryHandler()
    mysql_hook = get_connection_db()

    def get_data_mhs_periode(**kwargs):
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        dfMhs = script_kuliah_mhs.get_data_mhs_periode(kwargs.get("params").get("tahun_ajaran"), cursor)
        temp_file_path = file_temp_handler.generate_temporary_file('data_mahasiswa','csv')
        dfMhs.to_csv(temp_file_path, index=False)
        return temp_file_path

    def get_data_ips_mhs(**kwargs):
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        dfMhs = script_kuliah_mhs.get_data_ips_mhs(kwargs.get("params").get("tahun_ajaran"), cursor)
        temp_file_path = file_temp_handler.generate_temporary_file('data_ips','csv')
        dfMhs.to_csv(temp_file_path, index=False)
        return temp_file_path
    
    def get_data_pendaftar_wh(**kwargs):
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        dfMhs = script_kuliah_mhs.get_data_pendaftar_wh(cursor)
        temp_file_path = file_temp_handler.generate_temporary_file('data_pendaftar_wh','csv')
        dfMhs.to_csv(temp_file_path, index=False)
        return temp_file_path
    
    def get_data_mhs_wh(**kwargs):
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        dfMhs = script_kuliah_mhs.get_data_mhs_wh(cursor, kwargs.get("params").get("tahun_ajaran"))
        temp_file_path = file_temp_handler.generate_temporary_file('data_mhs_wh','csv')
        dfMhs.to_csv(temp_file_path, index=False)
        return temp_file_path
    
    def get_data_ipk_wh(**kwargs):
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        dfMhs = script_kuliah_mhs.get_data_ipk_wh(cursor, kwargs.get("params").get("tahun_ajaran"))
        temp_file_path = file_temp_handler.generate_temporary_file('data_ipk_wh','csv')
        dfMhs.to_csv(temp_file_path, index=False)
        return temp_file_path


    def add_ips_mhs_periode(**kwargs):
        ti = kwargs.get("ti")

        kuliah_mhs_file = ti.xcom_pull(task_ids='get_data_mhs', key=None)
        ips_mhs_file = ti.xcom_pull(task_ids='get_data_ips', key=None)

        paths = {
            "dfKuliah": kuliah_mhs_file,
            "dfIps": ips_mhs_file
        }

        dfKuliah = script_kuliah_mhs.add_ips_mhs_periode(kwargs.get("params").get("tahun_ajaran"), paths)
        temp_file_path = file_temp_handler.generate_temporary_file('dfKuliah','csv')
        dfKuliah.to_csv(temp_file_path, index=False)
        return temp_file_path

    def filter_angkatan_mhs(**kwargs):
        ti = kwargs.get("ti")

        kuliah_ips_mhs_file = ti.xcom_pull(task_ids='add_ips_mhs_periode', key=None)
        ips_mhs_file = ti.xcom_pull(task_ids='get_data_pendaftar_wh', key=None)

        paths = {
            "dfKuliahIps": kuliah_ips_mhs_file,
            "dfPendaftar": ips_mhs_file
        }
        dfMhs = script_kuliah_mhs.filter_angkatan_mhs(paths)
        temp_file_path = file_temp_handler.generate_temporary_file('data_ipk_wh','csv')
        dfMhs.to_csv(temp_file_path, index=False)
        return temp_file_path

    def add_ipk_semester(**kwargs):
        ti = kwargs.get("ti")

        kuliah_ips_filtered_file = ti.xcom_pull(task_ids='filter_angkatan_mhs', key=None)
        kuliah_mhs_wh_file = ti.xcom_pull(task_ids='get_data_mhs_wh', key=None)
        wh_ipk_file = ti.xcom_pull(task_ids='get_data_ipk_wh', key=None)

        paths = {
            "dfKuliahIpsFiltered": kuliah_ips_filtered_file,
            "df_wh_kuliah": kuliah_mhs_wh_file,
            "df_wh_ipk": wh_ipk_file
        }
        dfMhs = script_kuliah_mhs.add_ipk_semester(paths, kwargs.get("params").get("tahun_ajaran"))
        temp_file_path = file_temp_handler.generate_temporary_file('data_ipk_semester','csv')
        dfMhs.to_csv(temp_file_path, index=False)
        return temp_file_path


    def load_data(**kwargs):
        ti = kwargs.get("ti")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        data_file = ti.xcom_pull(task_ids='add_ipk_semester', key=None)
        script_kuliah_mhs.syncronize_to_db(data_file, cursor, conn)

        return "success"

    def clean_up(**kwargs):
        ti = kwargs.get("ti")
        kuliah_mhs_file = ti.xcom_pull(task_ids='get_data_mhs', key=None)
        ips_mhs_file = ti.xcom_pull(task_ids='get_data_ips', key=None)
        kuliah_ips_mhs_file = ti.xcom_pull(task_ids='add_ips_mhs_periode', key=None)
        ips_mhs_file_wh = ti.xcom_pull(task_ids='get_data_pendaftar_wh', key=None)
        kuliah_ips_filtered_file = ti.xcom_pull(task_ids='filter_angkatan_mhs', key=None)
        kuliah_mhs_wh_file = ti.xcom_pull(task_ids='get_data_mhs_wh', key=None)
        wh_ipk_file = ti.xcom_pull(task_ids='get_data_ipk_wh', key=None)
        data_file = ti.xcom_pull(task_ids='add_ipk_semester', key=None)

        temp_files = [kuliah_mhs_file, ips_mhs_file, kuliah_ips_mhs_file, ips_mhs_file_wh, kuliah_ips_filtered_file, kuliah_mhs_wh_file, kuliah_mhs_wh_file, wh_ipk_file, data_file]
        
        file_temp_handler.clear_temp_folder([temp_files])

    get_data_mhs_periode_task = PythonOperator(
        task_id='get_data_mhs',
        python_callable=get_data_mhs_periode,
        provide_context=True,
    )
    get_data_ips_mhs_task = PythonOperator(
        task_id='get_data_ips',
        python_callable=get_data_ips_mhs,
        provide_context=True,
    )
    get_data_pendaftar_wh_task = PythonOperator(
        task_id='get_data_pendaftar_wh',
        python_callable=get_data_pendaftar_wh,
        provide_context=True,
    )
    get_data_mhs_wh_task = PythonOperator(
        task_id='get_data_mhs_wh',
        python_callable=get_data_mhs_wh,
        provide_context=True,
    )
    get_data_ipk_wh_task = PythonOperator(
        task_id='get_data_ipk_wh',
        python_callable=get_data_ipk_wh,
        provide_context=True,
    )
    add_ips_mhs_periode_task = PythonOperator(
        task_id='add_ips_mhs_periode',
        python_callable=add_ips_mhs_periode,
        provide_context=True,
    )
    filter_angkatan_mhs_task = PythonOperator(
        task_id='filter_angkatan_mhs',
        python_callable=filter_angkatan_mhs,
        provide_context=True,
    )
    add_ipk_semester_task = PythonOperator(
        task_id='add_ipk_semester',
        python_callable=add_ipk_semester,
        provide_context=True,
    )
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )
    clean_up_task = PythonOperator(
        task_id='clean_up',
        python_callable=clean_up,
        provide_context=True,
    )

    [get_data_mhs_periode_task, get_data_ips_mhs_task] >> add_ips_mhs_periode_task

    [add_ips_mhs_periode_task, get_data_pendaftar_wh_task] >> filter_angkatan_mhs_task

    [filter_angkatan_mhs_task, get_data_mhs_wh_task, get_data_ipk_wh_task] >> add_ipk_semester_task 

    add_ipk_semester_task >> load_data_task

    load_data_task >> clean_up_task