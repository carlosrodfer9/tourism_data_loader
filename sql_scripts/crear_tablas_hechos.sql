CREATE TABLE Fact_Vuelo (
    ID_Vuelo SERIAL PRIMARY KEY,
    N_Pasajeros INT NOT NULL,
    ID_Fecha INT NOT NULL REFERENCES Dim_Fecha(ID_Fecha),
    ID_Aeropuerto_Origen INT NOT NULL REFERENCES Dim_Aeropuerto(ID_Aeropuerto),
    ID_Aeropuerto_Destino INT NOT NULL REFERENCES Dim_Aeropuerto(ID_Aeropuerto),
    ID_Hora_Salida INT NOT NULL REFERENCES Dim_Hora(D_Hora),
    ID_Hora_Llegada INT NOT NULL REFERENCES Dim_Hora(D_Hora),
    ID_Aerolinea INT NOT NULL REFERENCES Dim_Aerolinea(ID_Aerolinea)
);

CREATE TABLE Fact_Reserva (
    ID_Reserva SERIAL PRIMARY KEY,
    Precio_Noche NUMERIC(10, 2) NOT NULL,
    N_Noches INT NOT NULL,
    ID_Fecha INT NOT NULL REFERENCES Dim_Fecha(ID_Fecha),
    ID_Ciudad INT NOT NULL REFERENCES Dim_Ciudad(ID_Ciudad),
    ID_Alojamiento INT NOT NULL REFERENCES Dim_Alojamiento(ID_Alojamiento)
);

CREATE TABLE Fact_RedesSociales (
    ID_RedesSociales SERIAL PRIMARY KEY,
    N_MeGusta INT NOT NULL,
    N_Comentario INT NOT NULL,
    ID_Fecha INT NOT NULL REFERENCES Dim_Fecha(ID_Fecha),
    ID_Hora INT NOT NULL REFERENCES Dim_Hora(D_Hora),
    ID_Ubicacion INT NOT NULL REFERENCES Dim_Ubicacion(ID_Ubicacion)
);


CREATE TABLE Fact_Transporte (
    ID_Transporte SERIAL PRIMARY KEY,
    N_Personas INT NOT NULL,
    ID_Fecha INT NOT NULL REFERENCES Dim_Fecha(ID_Fecha),
    ID_Hora INT NOT NULL REFERENCES Dim_Hora(ID_Hora),
    ID_Ciudad INT NOT NULL REFERENCES Dim_Ciudad(ID_Ciudad),
    ID_Vehiculo INT NOT NULL REFERENCES Dim_Vehiculo(ID_Vehiculo),
    ID_Ubicacion INT NOT NULL REFERENCES Dim_Ubicacion(ID_Ubicacion)
);
