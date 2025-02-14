from confluent_kafka import Consumer, KafkaError
import json
import psycopg2
import utils

# Configuración del consumidor
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'transport',
    'auto.offset.reset': 'earliest'
}

def consume_live_data():
    consumer = Consumer(config)
    consumer.subscribe(['flight'])
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
            
            with utils.init_connection() as conn:
                cursor = conn.cursor()
                columns = [
                    "id_vuelo", 
                    "n_pasajeros", 
                    "id_fecha", 
                    "id_aeropuerto_origen", 
                    "id_aeropuerto_destino",
                    "id_hora_salida",
                    "id_hora_llegada",
                    "id_avion"
                ]
                try:
                    # Obtener las claves foráneas de las tablas de dimensiones (si ya existen)
                    n_pasajeros = data['N_Pasajeros']
                    fecha = data['ID_Fecha']
                    aeropuerto_origen = data['ID_Aeropuerto_Origen']
                    aeropuerto_dest = data['ID_Aeropuerto_Destino']
                    hora_salida = data['ID_Hora_Salida']
                    hora_llegada= data['ID_Hora_Llegada']
                    avion = data['ID_Avion']
                    id_vuelo = utils.encrypt_key([fecha, aeropuerto_origen, aeropuerto_dest, hora_salida, hora_llegada, avion])

                    utils.insert_values("fact_vuelo", columns, [id_vuelo, n_pasajeros, fecha, aeropuerto_origen, aeropuerto_dest, hora_salida, hora_llegada, avion], cursor)

                    print(f"Insertando datos de vuelo {id_vuelo}, {n_pasajeros}, {aeropuerto_origen}, {aeropuerto_dest}")
                except Exception as e:
                    print(f"Error al insertar datos: {e}")
                finally:
                    conn.commit()
                    cursor.close()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
