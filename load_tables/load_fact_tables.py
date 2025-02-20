import pandas as pd
import psycopg2
import datetime
import utils
import random
import time
import re


def load_flight_data(path: str):
    data = pd.read_csv(path, sep=";")

    flights = data.loc[:, [
        "dep_date",
        "dep_airport_name",
        "arr_airport_name",
        "dep_time",  
        "arr_time",
        "plane"
    ]]

    columns = [
        "id_vuelo", 
        "n_pasajeros", 
        "id_fecha", 
        "id_aeropuerto_origen", 
        "id_aeropuerto_destino",
        "id_hora_salida",
        "id_hora_llegada",
        "id_avion"
    ]

    with utils.init_connection() as conn:
        cursor = conn.cursor()
        try:
            for _, flight in flights.iterrows():
                if re.match('-', flight.loc["dep_date"]):
                    continue
                date_old = datetime.datetime.strptime(flight.loc["dep_date"], "%d/%m/%y")

                if re.match('-', flight.loc["dep_time"]):
                    dep_time = None
                else:
                    dep_time_old = datetime.datetime.strptime(flight.loc["dep_time"], "%H:%M")
                    dep_time = dep_time_old.strftime("%H:%M:%S")

                if re.match('-', flight.loc["arr_time"]):
                    arr_time = None
                else:
                    arr_time_old = datetime.datetime.strptime(flight.loc["arr_time"], "%H:%M")
                    arr_time = arr_time_old.strftime("%H:%M:%S")
                utils.insert_values(
                    "fact_vuelo",
                    columns,
                    [
                        utils.encrypt_key(flight.tolist()),
                        random.randint(30, 100),
                        date_old.strftime("%Y-%m-%d"),
                        utils.encrypt_key(flight.loc["dep_airport_name"]),
                        utils.encrypt_key(flight.loc["arr_airport_name"]),
                        dep_time,
                        arr_time,
                        utils.encrypt_key(str(flight.loc["plane"]).strip())
                    ],
                    cursor
                )
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            conn.commit()
            cursor.close()


def load_transport_data(path: str):
    df_vehiculo = pd.read_csv(path, sep=",")

    columns = [
        "id_transporte",
        "n_personas",
        "id_fecha",
        "id_hora",
        "id_ciudad",
        "id_vehiculo",
        "id_ubicacion"
    ]

    with utils.init_connection() as conn:
        cursor = conn.cursor()
        try:
            for _ in range(5000):
                # Seleccionar un vehículo aleatorio
                random_vehicle = df_vehiculo.sample(1).iloc[0]
                #id_vehiculo = str(random_vehicle["ID_Vehiculo"]).strip()
                mat = random_vehicle["Matricula"]
                categoria_vehiculo = random_vehicle["Categoria_Vehiculo"]
                id_vehiculo = utils.encrypt_key([mat, categoria_vehiculo])

                # Generar fecha aleatoria entre 2018 y 2024
                random_date = datetime.date(
                    random.randint(2018, 2024),
                    random.randint(1, 12),
                    random.randint(1, 28)
                ).strftime("%Y-%m-%d")

                # Generar hora aleatoria en formato HH:MM:SS
                random_hour = f"{random.randint(0, 23):02}:{random.randint(0, 59):02}:{random.randint(0, 59):02}"

                # Seleccionar Madrid
                id_Madrid = "d66bea2a59fb5bf1920d3ae717228f80"

                # Determinar el número de personas según la categoría del vehículo
                if categoria_vehiculo in ['Patinete', 'Bicicleta']:
                    n_personas = 1
                elif categoria_vehiculo in ['Autobus', 'Cercanias Renfe', 'Metro Ligero']:
                    n_personas = random.randint(1, 50)
                else:
                    n_personas = random.randint(1, 4)  # Otros vehículos, Taxi y VTC

                utils.insert_values(
                    "fact_transporte",
                    columns,
                    [
                        #utils.encrypt_key(f"{id_vehiculo}-{random_date}-{random_hour}"),
                        utils.encrypt_key([id_vehiculo, random_date, random_hour]),
                        n_personas,
                        random_date,
                        random_hour,
                        id_Madrid,
                        id_vehiculo,
                        1  # id_ubicacion
                    ],
                    cursor
                )
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            conn.commit()
            cursor.close()

    