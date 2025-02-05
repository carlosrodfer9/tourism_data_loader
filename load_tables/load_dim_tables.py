import pandas as pd
import psycopg2
from datetime import datetime
import utils


def load_city_data(path: str):
    data = pd.read_csv(path, sep=";")

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
    data = pd.read_csv(path, sep=";")

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
                utils.insert_values("dim_avion", columns, [utils.encrypt_key([mat, cat]), mat, cat], cursor)
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
        try:
            for airport in airports_all:
                airport = str(airport).strip()
                city = airport
                if airport == '':
                    break
                if '/' in city:
                    city = city.split('/')[0]
                elif '-' in city:
                    city = city.split('-')[0]
                utils.insert_values("dim_aeropuerto", columns, [utils.encrypt_key(airport), airport, None], cursor)
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
                if plane == '':
                    break
                utils.insert_values("dim_avion", columns, [utils.encrypt_key(plane), plane], cursor)
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            conn.commit()
            cursor.close()
    
