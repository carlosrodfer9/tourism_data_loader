from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime, timedelta

# Configuración del productor
config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(config)

# Generador de datos para Fact_Vuelo
def generate_flight_data():
    return {
        'N_Pasajeros': random.randint(50, 300),
        'ID_Fecha': random.randint(1, 365),  
        'ID_Aeropuerto_Origen': random.randint(1, 50), 
        'ID_Aeropuerto_Destino': random.randint(1, 50),
        'ID_Hora_Salida': random.randint(1, 1440),  # Hora en minutos del día
        'ID_Hora_Llegada': random.randint(1, 1440),
        'ID_Aerolinea': random.randint(1, 20)  
    }

# Envío de datos al tema 'flight_data'
for _ in range(10):  # Generar 10 mensajes
    data = generate_flight_data()
    producer.produce('flight_data', value=json.dumps(data).encode('utf-8'))
    print(f"Produciendo datos de vuelo: {data}")
    time.sleep(2)  # Pausa para simular la generación de datos

producer.flush()
