from confluent_kafka import Producer
import json
import random
import time

# Configuración del productor
config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(config)

# Generador de datos para Fact_Transporte
def generate_transport_data():
    return {
        'N_Personas': random.randint(1, 5),  
        'ID_Fecha': random.randint(1, 365), 
        'ID_Hora': random.randint(1, 1440),
        'ID_Ciudad': random.randint(1, 100),  
        'ID_Vehiculo': random.randint(1, 50), 
        'ID_Ubicacion': random.randint(1, 100)  
    }

# Envío de datos al tema 'transport_data'
for _ in range(10):  # Generar 10 mensajes
    data = generate_transport_data()
    producer.produce('transport_data', value=json.dumps(data).encode('utf-8'))
    print(f"Produciendo datos de transporte: {data}")
    time.sleep(2)

producer.flush()
