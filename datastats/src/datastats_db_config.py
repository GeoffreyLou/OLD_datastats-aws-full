import psycopg2
from datastats_variables_xyz import *

def get_db_connection():
    return psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )
