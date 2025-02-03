CREATE TABLE Dim_Ciudad (
    ID_Ciudad VARCHAR(100) PRIMARY KEY,
    Ciudad VARCHAR(100) NOT NULL,
    Pais VARCHAR(100) NOT NULL,
    Continente VARCHAR(50) NOT NULL
);

CREATE TABLE Dim_Aeropuerto (
    ID_Aeropuerto VARCHAR(100) PRIMARY KEY,
    Nombre VARCHAR(150) NOT NULL,
    ID_Ciudad VARCHAR(100) REFERENCES Dim_Ciudad(ID_Ciudad)
);

CREATE TABLE Dim_Hora (
    ID_Hora VARCHAR(100) PRIMARY KEY,
    Hora INT NOT NULL,
    Minuto INT NOT NULL,
    Segundo INT NOT NULL,
    AM BOOLEAN NOT NULL,
    Epoch BIGINT NOT NULL
);

CREATE TABLE Dim_Ubicacion (
    ID_Ubicacion VARCHAR(100) PRIMARY KEY,
    Ubicacion VARCHAR(200) NOT NULL,
    Latitud NUMERIC(10, 6) NOT NULL,
    Longitud NUMERIC(10, 6) NOT NULL,
    ID_Ciudad VARCHAR(100) NOT NULL REFERENCES Dim_Ciudad(ID_Ciudad)
);

CREATE TABLE Dim_Avion (
    ID_Avion VARCHAR(100) PRIMARY KEY,
    Avion VARCHAR(100) NOT NULL
);

CREATE TABLE Dim_Fecha (
    ID_Fecha VARCHAR(100) PRIMARY KEY,
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
    ID_Vehiculo VARCHAR(100) PRIMARY KEY,
    Matricula VARCHAR(20) NOT NULL UNIQUE,
    Categoria_Vehiculo VARCHAR(50) NOT NULL
);

CREATE TABLE Dim_Alojamiento (
    ID_Alojamiento VARCHAR(100) PRIMARY KEY,
    Nombre VARCHAR(100) NOT NULL,
    Tipo VARCHAR(50) NOT NULL
);
