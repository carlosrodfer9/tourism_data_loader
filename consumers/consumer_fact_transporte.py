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
    consumer.subscribe(['transport'])
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
                    'ID_Transporte',
                    'ID_Fecha',
                    'ID_Hora',
                    'ID_Ciudad',
                    'ID_Vehiculo',
                    'ID_Ubicacion',
                    'N_Personas'
                ]
                try:
                    # Obtener las claves foráneas de las tablas de dimensiones (si ya existen)
                    id_fecha = data['ID_Fecha']
                    id_hora = data['ID_Hora']
                    id_ciudad = data['ID_Ciudad']
                    id_vehiculo = data['ID_Vehiculo']
                    id_ubicacion = data['ID_Ubicacion']
                    n_personas = data['N_Personas']
                    id_transporte = utils.encrypt_key([id_fecha, id_hora, id_ciudad, id_vehiculo, id_ubicacion, n_personas])

                    utils.insert_values("fact_transporte", columns, [id_transporte, id_fecha, id_hora, id_ciudad, id_vehiculo, id_ubicacion, n_personas], cursor)

                    print(f"Insertando datos de vuelo {id_transporte}, {n_personas}, {id_ciudad}, {id_fecha}")
                except Exception as e:
                    print(f"Error al insertar datos: {e}")
                finally:
                    conn.commit()
                    cursor.close()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
