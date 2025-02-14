import psycopg2
from datetime import datetime
import hashlib


def encrypt_key(values: list):
    cryptor = hashlib.md5()
    for value in values:
        cryptor.update(str(value).encode('UTF-8'))
    output = cryptor.hexdigest()
    return output


def init_connection():
    conn = psycopg2.connect(
        host='localhost',
        user='postgres',
        password='postgres',
        database='tourism'
    )
    return conn


def insert_values(table_name: str, columns: list[str], values: list[str], cursor):
    try:
        v = []
        for i in range(len(values)):
            v.append(r"%s")
        v = ','.join(str(x) for x in v)
        sql_query = f'''
            INSERT INTO {table_name} ({','.join(str(x) for x in columns)})
            VALUES ({v}) 
            ON CONFLICT ({columns[0]}) DO NOTHING;'''
        cursor.execute(sql_query, tuple(values))

        print(f"Datos insertados en la base de datos: {','.join(str(x) for x in values)}")
    except Exception as e:
        print(f"Error al insertar datos: {e}")
        raise e
    

def read_values(table_name: str, columns: list[str], cursor):
    try:
        sql_query = f'''
            SELECT {','.join(str(x) for x in columns)} FROM {table_name} 
        '''
        cursor.execute(sql_query)
        values = cursor.fetchall()

        print(f"Datos obtenidos de la base de datos: {','.join(str(x) for x in columns)}")

        return values
    except Exception as e:
        print(f"Error al leer datos: {e}")
        raise e
    

def read_values_with_condition(table_name: str, columns: list[str], condition: str, cursor):
    try:
        sql_query = f'''
            SELECT {','.join(str(x) for x in columns)} FROM {table_name} 
            WHERE {condition}
        '''
        cursor.execute(sql_query)
        values = cursor.fetchall()

        print(f"Datos obtenidos de la base de datos: {','.join(str(x) for x in columns)}")

        return values
    except Exception as e:
        print(f"Error al insertar datos: {e}")
        raise e