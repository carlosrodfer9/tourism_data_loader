from confluent_kafka import Producer
import json
import random
import time

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

# Envío de datos al tema 'reservation_data'
for _ in range(10):  # Generar 10 mensajes
    data = generate_reservation_data()
    producer.produce('reservation_data', value=json.dumps(data).encode('utf-8'))
    print(f"Produciendo datos de reserva: {data}")
    time.sleep(2)

producer.flush()

csjadocijsdicjdsoakcds
