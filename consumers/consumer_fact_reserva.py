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
    consumer.subscribe(['reservation'])
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
                    'Precio_Noche',
                    'N_Noches',
                    'ID_Fecha',
                    'ID_Ciudad',
                    'ID_Alojamiento'
                ]
                try:
                    # Obtener las claves foráneas de las tablas de dimensiones (si ya existen)
                    precio = data['Precio_Noche']
                    n_noches = data['N_Noches']
                    id_fecha = data['ID_Fecha']
                    id_ciudad = data['ID_Ciudad']
                    id_alojamiento = data['ID_Alojamiento']
                    id_reserva = utils.encrypt_key([precio, n_noches, id_fecha, id_ciudad, id_alojamiento])

                    utils.insert_values("fact_reserva", columns, [id_reserva, precio, n_noches, id_fecha, id_ciudad, id_alojamiento], cursor)

                    print(f"Insertando datos de vuelo {id_reserva}, {id_alojamiento}, {id_ciudad}, {id_fecha}")
                except Exception as e:
                    print(f"Error al insertar datos: {e}")
                finally:
                    conn.commit()
                    cursor.close()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
