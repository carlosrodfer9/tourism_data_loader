CREATE TABLE Dim_Ciudad (
    ID_Ciudad VARCHAR PRIMARY KEY,
    Ciudad VARCHAR(100) NOT NULL,
    Pais VARCHAR(100) NOT NULL,
    Continente VARCHAR(50) NOT NULL
);

CREATE TABLE Dim_Aeropuerto (
    ID_Aeropuerto VARCHAR PRIMARY KEY,
    Nombre VARCHAR(150) NOT NULL,
    ID_Ciudad INT REFERENCES Dim_Ciudad(ID_Ciudad)
);

CREATE TABLE Dim_Hora (
    ID_Hora TEXT PRIMARY KEY,  
    Hora INT NOT NULL,
    Minuto INT NOT NULL,
    Segundo INT NOT NULL,
    AM BOOLEAN NOT NULL,
    Epoch BIGINT NOT NULL
);

CREATE TABLE Dim_Ubicacion (
    ID_Ubicacion VARCHAR PRIMARY KEY,
    Ubicacion VARCHAR(200) NOT NULL,
    Latitud NUMERIC(10, 6) NOT NULL,
    Longitud NUMERIC(10, 6) NOT NULL,
    ID_Ciudad INT NOT NULL REFERENCES Dim_Ciudad(ID_Ciudad)
);

CREATE TABLE Dim_Avion (
    ID_Avion VARCHAR PRIMARY KEY,
    Aerolinea VARCHAR(100) NOT NULL
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
