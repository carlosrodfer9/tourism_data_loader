import pandas as pd
import psycopg2
from datetime import datetime
import utils
import random


def load_flight_data(path: str):
    data = pd.read_csv(path, sep=";")

    flights = data.loc[:, [
        "plane", 
        "dep_date", 
        "dep_time", 
        "dep_airport_name", 
        "arr_time", 
        "arr_airport_name"
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
                utils.insert_values(
                    "fact_vuelo",
                    columns,
                    [
                        utils.encrypt_key(flight.tolist()),
                        random.randint(30, 100),
                        flight.loc["dep_date"],
                        utils.encrypt_key(flight.loc["dep_airport_name"]),
                        utils.encrypt_key(flight.loc["arr_airport_name"]),
                        flight.loc["dep_time"],
                        flight.loc["arr_time"],
                        utils.encrypt_key(str(flight.loc["plane"]).strip())
                    ],
                    cursor
                )
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            conn.commit()
            cursor.close()

    