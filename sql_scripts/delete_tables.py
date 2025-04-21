import psycopg2

def borrar_tablas():
    # Configuración de la conexión
    conn = psycopg2.connect(
        dbname="tourism",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    try:
        # Borrar tablas en orden correcto
        cursor.execute('''
        DROP TABLE IF EXISTS Fact_Vuelo CASCADE;
        DROP TABLE IF EXISTS Fact_Reserva CASCADE;
        DROP TABLE IF EXISTS Fact_RedesSociales CASCADE;
        DROP TABLE IF EXISTS Fact_Transporte CASCADE;
        
        DROP TABLE IF EXISTS Dim_Ciudad CASCADE;
        DROP TABLE IF EXISTS Dim_Aeropuerto CASCADE;
        DROP TABLE IF EXISTS Dim_Hora CASCADE;
        DROP TABLE IF EXISTS Dim_Ubicacion CASCADE;
        DROP TABLE IF EXISTS Dim_Aerolinea CASCADE;
        DROP TABLE IF EXISTS Dim_Fecha CASCADE;
        DROP TABLE IF EXISTS Dim_Vehiculo CASCADE;
        DROP TABLE IF EXISTS Dim_Alojamiento CASCADE;
        ''')
        
        # Confirmar cambios
        conn.commit()
        print("Tablas eliminadas exitosamente.")
    
    except Exception as e:
        print("Error al eliminar tablas:", e)
        conn.rollback()
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    borrar_tablas()
