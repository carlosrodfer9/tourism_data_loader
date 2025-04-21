from confluent_kafka import Producer
import json
import random
import time
import datetime
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
        'ID_Fecha': datetime.date.today().isoformat(),  
        'ID_Aeropuerto_Origen': airports[random.randint(1, len(airports)-1)][0], 
        'ID_Aeropuerto_Destino': airports[random.randint(1, len(airports)-1)][0],
        'ID_Hora_Salida': (datetime.datetime.now() - datetime.timedelta(hours=random.randint(0,7))).strftime("%H:%M:%S"),
        'ID_Hora_Llegada': datetime.time(datetime.datetime.now().hour, datetime.datetime.now().minute, datetime.datetime.now().second).isoformat(),
        'ID_Avion': planes[random.randint(1, len(planes)-1)][0] 
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