from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.models.param import Param, ParamsDict

import sys
sys.path.insert(0, '/opt/airflow')
from wh_script import get_connection_db

with DAG(
    dag_id="utils_test_connection_db",
)as dag:
    mysql_hook = get_connection_db()

    def test_hooks_db(**kwargs):
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        query = "SELECT 'Test Connection'"
        cursor.execute(query)
        return "success"

    test_development_db_task = MySqlOperator(
        task_id="test_connection_development", sql="SELECT 'Test Connection'", dag=dag, mysql_conn_id="mysql_dev_conn"
    )
    test_production_db_task = MySqlOperator(
        task_id="test_connection_production", sql="SELECT 'Test Connection'", dag=dag, mysql_conn_id="mysql_prod_conn"
    )
    test_connection_db_hooks = PythonOperator(
        task_id="test_connection_hooks",
        python_callable=test_hooks_db,
    )

    [test_development_db_task, test_production_db_task] >> test_connection_db_hooks