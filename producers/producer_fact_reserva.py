from confluent_kafka import Producer
import json
import random
import time
import utils
import datetime

# Configuración del productor
config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(config)

# Generador de datos para Fact_Reserva
def generate_reservation_data():
    return {
        'Precio_Noche': round(random.uniform(50.0, 500.0), 2),
        'N_Noches': random.randint(1, 15),
        'ID_Fecha': random.randint(1, 365),  
        'ID_Ciudad': random.randint(1, 100),  
        'ID_Alojamiento': random.randint(1, 50) 
    }

cities: list
accomodations: list

def _read_values_from_db():
    with utils.init_connection() as conn:
        cursor = conn.cursor()
        try:
            global cities
            global accomodations

            cities = utils.read_values("dim_ciudad", ["id_ciudad"], cursor)
            accomodations =  utils.read_values("dim_alojamiento", ["id_alojamiento"], cursor)     
        except Exception as e:
            print(f"Error al leer datos: {e}")
        finally:
            cursor.close()


# Generador de datos para Fact_Vuelo
def _generate_reservation_data():
    return {
        'Precio_Noche': round(random.uniform(50.0, 500.0), 2),
        'N_Noches': random.randint(1, 15),
        'ID_Fecha': datetime.date.today().isoformat(),  
        'ID_Ciudad': cities[random.randint(1, len(cities))][0],  
        'ID_Alojamiento': accomodations[random.randint(1, len(accomodations))][0] 
    }

def produce_live_data():
    # Envío de datos al tema 'flight_data'
    _read_values_from_db()
    for i in range(10):  # Generar 10 mensajes
        data = _generate_reservation_data()
        producer.produce('reservation', value=json.dumps(data).encode('utf-8'))
        print(f"Produciendo datos de reservas: {data}")
        time.sleep(10)  # Pausa para simular la generación de datos
    producer.flush()
