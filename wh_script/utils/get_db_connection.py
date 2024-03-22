import os
# from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

def get_connection_db():
    is_production = os.environ['AIRFLOW_DB_PRODUCTION']

    if is_production == "true":
        print("Using Production Environtment Database")
        return MySqlHook(mysql_conn_id="mysql_prod_conn")
    elif is_production == "false":
        print("Using Development Environtment Database")
        return MySqlHook(mysql_conn_id="mysql_dev_conn")
    else:
        raise Exception("Please fill the AIRFLOW_DB_PRODUCTION with value of 'true' or 'false'")

get_connection_db()