import json
import random
import time
from datetime import datetime
import pandas as pd
from confluent_kafka import Producer

# Configuración del productor
config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(config)

# Cargar los datos de las tablas de dimensiones desde los CSVs necesarios
df_vehiculo = pd.read_csv('dim_vehiculo.csv')
df_ciudad = pd.read_csv('dim_ciudad.csv')
id_fecha = int(datetime.today().strftime('%Y%m%d'))  # Formato: YYYYMMDD como entero ; Fecha actual
#id_hora = int(datetime.now().strftime('%H%M%S'))     # Formato: HHMMSS como entero ; hora actual
id_hora = datetime.now().strftime('%H%M%S')  # Formato: HHMMSS como texto

# Generador de datos para Fact_Transporte
def generate_transport_data():
    # Obtener valores aleatorios para id_ciudad y id_vehiculo
    id_ciudad = random.choice(df_ciudad['ID_Ciudad'].values)
    id_vehiculo = random.choice(df_vehiculo['ID_Vehiculo'].values)
    
    # Obtener la categoría del vehículo
    categoria_vehiculo = df_vehiculo.loc[df_vehiculo['ID_Vehiculo'] == id_vehiculo, 'Categoria_Vehiculo'].values[0]
    
    # Determinar el número de personas según la categoría del vehículo
    if categoria_vehiculo in ['Patinete', 'Bicicleta']:
        n_personas = 1
    elif categoria_vehiculo in ['Autobus', 'Cercanias Renfe', 'Metro Ligero']:
        n_personas = random.randint(1, 50)  
    else:
        n_personas = random.randint(1, 4)  # Otros vehículos, Taxi y VTC
    
    return {
        'N_Personas': int(n_personas),
        'ID_Fecha': id_fecha,
        'ID_Hora':id_hora,
        'ID_Ciudad': int(id_ciudad),
        'ID_Vehiculo': int(id_vehiculo),
        'ID_Ubicacion': random.randint(1, 100)  # Asignar una ubicación aleatoria
    }

# Envío de datos al tema 'transport_data'
for _ in range(10):  # Generar 10 mensajes
    data = generate_transport_data()
    producer.produce('transport', value=json.dumps(data).encode('utf-8'))
    print(f"Produciendo datos de transporte: {data}")
    time.sleep(2)

producer.flush()
