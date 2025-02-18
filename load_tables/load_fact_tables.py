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

    