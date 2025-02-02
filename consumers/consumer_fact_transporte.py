from confluent_kafka import Consumer, KafkaError
import json
import psycopg2

# Configuración del consumidor
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'transport',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)
consumer.subscribe(['transport'])

# Conectar a la base de datos PostgreSQL
conn = psycopg2.connect(
    host='localhost',
    user='postgres',
    password='1234',
    database='postgres'
)
cursor = conn.cursor()

print("Esperando datos de transporte...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        # Decodificar el mensaje recibido
        data = json.loads(msg.value().decode('utf-8'))

        # Obtener las claves foráneas de las tablas de dimensiones (si ya existen)
        id_fecha = data['ID_Fecha']
        id_hora = data['ID_Hora']
        id_ciudad = data['ID_Ciudad']
        id_vehiculo = data['ID_Vehiculo']
        id_ubicacion = data['ID_Ubicacion']
        n_personas = data['N_Personas']

        # Insertar los datos en la tabla de hechos Fact_Transporte
        cursor.execute('''
            INSERT INTO Fact_Transporte (N_Personas, ID_Fecha, ID_Hora, ID_Ciudad, ID_Vehiculo, ID_Ubicacion)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (n_personas, id_fecha, id_hora, id_ciudad, id_vehiculo, id_ubicacion))

        conn.commit()
        print(f"Datos de transporte insertados: {data}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    cursor.close()
    conn.close()
