import json
import random
import time
import datetime
import pandas as pd
from confluent_kafka import Producer
import utils

# Configuración del productor
config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(config)

cities: list
vehicles: list

def _read_values_from_db():
    with utils.init_connection() as conn:
        cursor = conn.cursor()
        try:
            global cities
            global vehicles

            cities = utils.read_values("dim_ciudad", ["id_ciudad"], cursor)
            vehicles =  utils.read_values("dim_vehiculo", ["id_vehiculo", "categoria_vehiculo"], cursor)     
        except Exception as e:
            print(f"Error al leer datos: {e}")
        finally:
            cursor.close()


# Generador de datos para Fact_Vuelo
def _generate_transport_data():
    vehicle = vehicles[random.randint(1, len(vehicles))]

    if vehicle[1] in ['Patinete', 'Bicicleta']:
        n_personas = 1
    elif vehicle[1] in ['Autobus', 'Cercanias Renfe', 'Metro Ligero']:
        n_personas = random.randint(1, 50)
    else:
        n_personas = random.randint(1, 4)  # Otros vehículos, Taxi y VTC

    return {
        'N_Personas': int(n_personas),
        'ID_Fecha': datetime.date.today().isoformat(),
        'ID_Hora':datetime.time(datetime.datetime.now().day, datetime.datetime.now().minute, datetime.datetime.now().second).isoformat(),
        'ID_Ciudad': cities[random.randint(1, len(cities))][0],
        'ID_Vehiculo': vehicle[0],
        'ID_Ubicacion': None
    }

def produce_live_data(n_messages: int):
    # Envío de datos al tema 'flight_data'
    _read_values_from_db()
    for i in range(n_messages):  # Generar 10 mensajes
        data = _generate_transport_data()
        producer.produce('transport', value=json.dumps(data).encode('utf-8'))
        print(f"Produciendo datos de transporte: {data}")
        time.sleep(10)  # Pausa para simular la generación de datos
    producer.flush()
