from confluent_kafka import Producer
import json
import random
import time

# Configuración del productor
config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(config)

# Generador de datos para Fact_RedesSociales
def generate_social_data():
    return {
        'N_MeGusta': random.randint(0, 500),
        'N_Comentario': random.randint(0, 50),
        'ID_Fecha': random.randint(1, 365), 
        'ID_Hora': random.randint(1, 1440), 
        'ID_Ubicacion': random.randint(1, 100)  
    }

# Envío de datos al tema 'social_data'
for _ in range(10):  # Generar 10 mensajes
    data = generate_social_data()
    producer.produce('social_data', value=json.dumps(data).encode('utf-8'))
    print(f"Produciendo datos de redes sociales: {data}")
    time.sleep(2)

producer.flush()
