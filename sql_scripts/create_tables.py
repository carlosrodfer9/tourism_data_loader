import psycopg2


def crear_tablas():
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
        # Crear tablas de dimensiones
        cursor.execute('''
            CREATE TABLE Dim_Ciudad (
                ID_Ciudad VARCHAR PRIMARY KEY,
                Ciudad VARCHAR(100) NOT NULL,
                Pais VARCHAR(100) NOT NULL,
                Continente VARCHAR(50) NOT NULL
            );

            CREATE TABLE Dim_Aeropuerto (
                ID_Aeropuerto VARCHAR PRIMARY KEY,
                Nombre VARCHAR(150) NOT NULL,
                ID_Ciudad VARCHAR REFERENCES Dim_Ciudad(ID_Ciudad)
            );

            CREATE TABLE Dim_Hora (
                ID_Hora TEXT PRIMARY KEY,
                Hora INT NOT NULL,
                Minuto INT NOT NULL,
                Segundo INT NOT NULL,
                AM BOOLEAN NOT NULL,
                Epoch BIGINT NOT NULL
            );

            CREATE TABLE Dim_Avion (
                ID_Avion VARCHAR PRIMARY KEY,
                Avion VARCHAR(100) NOT NULL
            );

            CREATE TABLE Dim_Fecha (
                ID_Fecha VARCHAR PRIMARY KEY,
                Dia INT NOT NULL,
                Mes INT NOT NULL,
                Año INT NOT NULL,
                NombreDia VARCHAR(50) NOT NULL,
                NombreMes VARCHAR(50) NOT NULL,
                DiaSemana INT NOT NULL,
                Trimestre INT NOT NULL,
                MesTrimestre INT NOT NULL
            );

            CREATE TABLE Dim_Vehiculo (
                ID_Vehiculo VARCHAR PRIMARY KEY,
                Matricula VARCHAR(20),
                Categoria_Vehiculo VARCHAR(50) NOT NULL
            );

            CREATE TABLE Dim_Alojamiento (
                ID_Alojamiento VARCHAR PRIMARY KEY,
                Nombre VARCHAR(100) NOT NULL,
                Tipo VARCHAR(50) NOT NULL,
                ID_Ciudad VARCHAR REFERENCES Dim_Ciudad(ID_Ciudad)
            );

            CREATE TABLE Fact_Vuelo (
                ID_Vuelo VARCHAR PRIMARY KEY,
                N_Pasajeros INT NOT NULL,
                ID_Fecha VARCHAR NOT NULL REFERENCES Dim_Fecha(ID_Fecha),
                ID_Aeropuerto_Origen VARCHAR NOT NULL REFERENCES Dim_Aeropuerto(ID_Aeropuerto),
                ID_Aeropuerto_Destino VARCHAR NOT NULL REFERENCES Dim_Aeropuerto(ID_Aeropuerto),
                ID_Hora_Salida VARCHAR REFERENCES Dim_Hora(ID_Hora),
                ID_Hora_Llegada VARCHAR REFERENCES Dim_Hora(ID_Hora),
                ID_Avion VARCHAR REFERENCES Dim_Avion(ID_Avion)
            );

            CREATE TABLE Fact_Reserva (
                ID_Reserva VARCHAR PRIMARY KEY,
                Precio_Noche NUMERIC(10, 2) NOT NULL,
                N_Noches INT NOT NULL,
                ID_Fecha VARCHAR NOT NULL REFERENCES Dim_Fecha(ID_Fecha),
                ID_Ciudad VARCHAR NOT NULL REFERENCES Dim_Ciudad(ID_Ciudad),
                ID_Alojamiento VARCHAR NOT NULL REFERENCES Dim_Alojamiento(ID_Alojamiento)
            );

            CREATE TABLE Fact_Transporte (
                ID_Transporte VARCHAR PRIMARY KEY,
                N_Personas INT NOT NULL,
                ID_Fecha VARCHAR NOT NULL REFERENCES Dim_Fecha(ID_Fecha),
                ID_Hora TEXT NOT NULL REFERENCES Dim_Hora(ID_Hora),
                ID_Ciudad VARCHAR NOT NULL REFERENCES Dim_Ciudad(ID_Ciudad),
                ID_Vehiculo VARCHAR NOT NULL REFERENCES Dim_Vehiculo(ID_Vehiculo),
                ID_Ubicacion INT
            );
        ''')

        # Confirmar cambios
        conn.commit()
        print("Tablas creadas exitosamente.")

    except Exception as e:
        print("Error al crear tablas:", e)
        conn.rollback()

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    crear_tablas()
