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
    ID_Reserva SERIAL PRIMARY KEY,
    Precio_Noche NUMERIC(10, 2) NOT NULL,
    N_Noches INT NOT NULL,
    ID_Fecha INT NOT NULL REFERENCES Dim_Fecha(ID_Fecha),
    ID_Ciudad INT NOT NULL REFERENCES Dim_Ciudad(ID_Ciudad),
    ID_Alojamiento INT NOT NULL REFERENCES Dim_Alojamiento(ID_Alojamiento)
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
