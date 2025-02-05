CREATE TABLE Fact_Vuelo (
    ID_Vuelo VARCHAR(100) PRIMARY KEY,
    N_Pasajeros INT NOT NULL,
    ID_Fecha VARCHAR(100) NOT NULL REFERENCES Dim_Fecha(ID_Fecha),
    ID_Aeropuerto_Origen VARCHAR(100) NOT NULL REFERENCES Dim_Aeropuerto(ID_Aeropuerto),
    ID_Aeropuerto_Destino VARCHAR(100) NOT NULL REFERENCES Dim_Aeropuerto(ID_Aeropuerto),
    ID_Hora_Salida VARCHAR(100) NOT NULL REFERENCES Dim_Hora(ID_Hora),
    ID_Hora_Llegada VARCHAR(100) NOT NULL REFERENCES Dim_Hora(ID_Hora),
    ID_Avion VARCHAR(100) NOT NULL REFERENCES Dim_Avion(ID_Avion)
);

CREATE TABLE Fact_Reserva (
    ID_Reserva VARCHAR(100) PRIMARY KEY,
    Precio_Noche NUMERIC(10, 2) NOT NULL,
    N_Noches INT NOT NULL,
    ID_Fecha VARCHAR(100) NOT NULL REFERENCES Dim_Fecha(ID_Fecha),
    ID_Ciudad VARCHAR(100) NOT NULL REFERENCES Dim_Ciudad(ID_Ciudad),
    ID_Alojamiento VARCHAR(100) NOT NULL REFERENCES Dim_Alojamiento(ID_Alojamiento)
);

CREATE TABLE Fact_RedesSociales (
    ID_RedesSociales VARCHAR(100) PRIMARY KEY,
    N_MeGusta INT NOT NULL,
    N_Comentario INT NOT NULL,
    ID_Fecha VARCHAR(100) NOT NULL REFERENCES Dim_Fecha(ID_Fecha),
    ID_Hora VARCHAR(100) NOT NULL REFERENCES Dim_Hora(ID_Hora),
    ID_Ubicacion VARCHAR(100) NOT NULL REFERENCES Dim_Ubicacion(ID_Ubicacion)
);


CREATE TABLE Fact_Transporte (
    ID_Transporte VARCHAR(100) PRIMARY KEY,
    N_Personas INT NOT NULL,
    ID_Fecha VARCHAR(100) NOT NULL REFERENCES Dim_Fecha(ID_Fecha),
    ID_Hora VARCHAR(100) NOT NULL REFERENCES Dim_Hora(ID_Hora),
    ID_Ciudad VARCHAR(100) NOT NULL REFERENCES Dim_Ciudad(ID_Ciudad),
    ID_Vehiculo VARCHAR(100) NOT NULL REFERENCES Dim_Vehiculo(ID_Vehiculo),
    ID_Ubicacion VARCHAR(100) NOT NULL REFERENCES Dim_Ubicacion(ID_Ubicacion)
);
