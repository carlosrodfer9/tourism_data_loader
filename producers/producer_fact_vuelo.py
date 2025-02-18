from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime, timedelta
import utils

# Configuración del productor
config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(config)

airports: list
planes: list

def _read_values_from_db():
    with utils.init_connection() as conn:
        cursor = conn.cursor()
        try:
            global airports
            global planes

            airports = utils.read_values("dim_aeropuerto", ["id_aeropuerto"], cursor)
            planes =  utils.read_values("dim_avion", ["id_avion"], cursor)     
        except Exception as e:
            print(f"Error al leer datos: {e}")
        finally:
            cursor.close()


# Generador de datos para Fact_Vuelo
def _generate_flight_data():
    print(airports[random.randint(1, len(airports)-1)])
    print(airports[random.randint(1, len(airports)-1)][0])
    return {
        'N_Pasajeros': random.randint(50, 300),
        'ID_Fecha': random.randint(1, 365),  
        'ID_Aeropuerto_Origen': airports[random.randint(1, len(airports)-1)][0], 
        'ID_Aeropuerto_Destino': airports[random.randint(1, len(airports)-1)][0],
        'ID_Hora_Salida': random.randint(1, 1440),  # Hora en minutos del día
        'ID_Hora_Llegada': random.randint(1, 1440),
        'ID_Avion': planes[random.randint(1, len(planes))][0] 
    }

def produce_live_data(n_messages: int):
    # Envío de datos al tema 'flight'
    _read_values_from_db()
    for i in range(n_messages):  # Generar 10 mensajes
        data = _generate_flight_data()
        producer.produce('flight', value=json.dumps(data).encode('utf-8'))
        print(f"Produciendo datos de vuelo: {data}")
        time.sleep(10)  # Pausa para simular la generación de datos
    producer.flush()