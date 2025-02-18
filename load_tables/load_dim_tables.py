import pandas as pd
import psycopg2
import datetime
import utils
from faker import Faker
import random
import re


def load_hour_data():
    with utils.init_connection() as conn:
        cursor = conn.cursor()
        columns = ["ID_Hora", "Hora", "Minuto", "Segundo", "AM", "Epoch"]
        try:
            for hora in range(24):
                for minuto in range(60):
                    for segundo in range(60):
                        am = hora < 12
                        epoch = hora * 3600 + minuto * 60 + segundo
                        id_hora = datetime.time(hora, minuto, segundo).isoformat()

                        utils.insert_values(
                            "dim_hora", 
                            columns, 
                            [id_hora, hora, minuto, segundo, am, epoch], 
                            cursor
                        )

        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            conn.commit()
            cursor.close()



def load_date_data():
    # dates de inicio y fin
    date = datetime.date(2018, 1, 1)
    fin_date = datetime.date(2030, 12, 31)

    columns = ["ID_Fecha", "Dia", "Mes", "Año", "NombreDia", "NombreMes", "DiaSemana", "Trimestre", "MesTrimestre"]

    # Insertar datos
    with utils.init_connection() as conn:
        cursor = conn.cursor()
        try:
            while date <= fin_date:
                id_date = date.isoformat() #usamos la propia fecha como id
                day_name = date.strftime('%A')
                month_name = date.strftime('%B')
                day_week = date.weekday()  # 0=lunes, 1=martes, ..., 6=domingo
                quarter = (date.month - 1) // 3 + 1
                quarter_month = (date.month - 1) % 3 + 1

                utils.insert_values(
                    "dim_fecha", 
                    columns, 
                    [id_date, date.day, date.month, date.year, day_name, month_name, day_week, quarter, quarter_month], 
                    cursor
                )

                # Incrementar un día
                date += datetime.timedelta(days=1)
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            conn.commit()
            cursor.close()


def load_accomodation_data(path: str):
    fake = Faker()
    data = pd.read_csv(path, sep=";", encoding="latin")

    accomodations = data.loc[:, ["alojamiento_tipo", "denominacion"]]
    types = data.loc[:, "alojamiento_tipo"].unique().tolist()

    columns = ["id_alojamiento", "nombre", "tipo", "id_ciudad"]
    with utils.init_connection() as conn:
        cursor = conn.cursor()
        cities = utils.read_values("dim_ciudad", ["id_ciudad", "ciudad"], cursor)
        try:
            for city in cities:
                if str(city[1]).upper() == "MADRID":
                    id_madrid = city[0]
                    break
            for _, row in accomodations.iterrows():
                denom = str(row.loc["denominacion"]).strip()
                if denom == '':
                    break
                tipo = str(row.loc["alojamiento_tipo"]).strip()
                if tipo == 'VIVIENDAS DE USO TU':
                    tipo = "VIVIENDA TURISTICA"
                utils.insert_values("dim_alojamiento", columns, [utils.encrypt_key([denom, tipo, id_madrid]), denom, tipo, id_madrid], cursor)
            for city in cities:
                for i in range(15):
                    id_city = city[0]
                    tipo = types[random.randint(0, len(types) - 1)]
                    denom = fake.name()
                    utils.insert_values("dim_alojamiento", columns, [utils.encrypt_key([denom, tipo, id_city]), denom, tipo, id_city], cursor)
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            conn.commit()
            cursor.close()


def load_city_data(path: str):
    data = pd.read_csv(path, sep=",")

    cities = data.loc[:, ["Ciudad", "Pais", "Continente"]]

    columns = ["id_ciudad", "ciudad", "pais", "continente"]
    with utils.init_connection() as conn:
        cursor = conn.cursor()
        try:
            for _, row in cities.iterrows():
                city = str(row.loc["Ciudad"]).strip()
                if city == '':
                    break
                country = str(row.loc["Pais"]).strip()
                if country == '':
                    break
                continent = str(row.loc["Continente"]).strip()
                if continent == '':
                    break
                utils.insert_values("dim_ciudad", columns, [utils.encrypt_key([city, country, continent]), city, country, continent], cursor)
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            conn.commit()
            cursor.close()

def load_vehicle_data(path: str):
    data = pd.read_csv(path, sep=",")

    vehicles = data.loc[:, ["Matricula", "Categoria_Vehiculo"]]

    columns = ["id_vehiculo", "matricula", "categoria_vehiculo"]
    with utils.init_connection() as conn:
        cursor = conn.cursor()
        try:
            for _, row in vehicles.iterrows():
                mat = str(row.loc["Matricula"]).strip()
                if mat == '':
                    break
                cat = str(row.loc["Categoria_Vehiculo"]).strip()
                if cat == '':
                    break
                utils.insert_values("dim_vehiculo", columns, [utils.encrypt_key([mat, cat]), mat, cat], cursor)
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            conn.commit()
            cursor.close()


def load_airport_data(path: str):
    data = pd.read_csv(path, sep=";")

    airports_dep = data.loc[:, "dep_airport_name"].unique().tolist()
    airports_arr = data.loc[:, "arr_airport_name"].unique().tolist()
    airports_all = set(airports_dep + airports_arr)

    columns = ["id_aeropuerto", "nombre", "id_ciudad"]
    with utils.init_connection() as conn:
        cursor = conn.cursor()
        cities = utils.read_values("dim_ciudad", ["id_ciudad", "ciudad"], cursor)
        try:
            for airport in airports_all:
                airport = str(airport).strip()
                city = None
                for c in cities:
                    if re.match(str(c[1]).upper(), airport):
                        city = c[0]
                        break
                if airport == '':
                    break
                utils.insert_values("dim_aeropuerto", columns, [utils.encrypt_key(airport), airport, city], cursor)
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            conn.commit()
            cursor.close()


def load_plane_data(path: str):
    data = pd.read_csv(path, sep=";")

    planes = data.loc[:, "plane"].unique().tolist()

    columns = ["id_avion", "avion"]
    with utils.init_connection() as conn:
        cursor = conn.cursor()
        try:
            for plane in planes:
                plane = str(plane).strip()
                utils.insert_values("dim_avion", columns, [utils.encrypt_key(plane), plane], cursor)
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            conn.commit()
            cursor.close()
    
