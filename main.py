import argparse
import sql_scripts.create_tables as create_tables
import sql_scripts.delete_tables as delete_tables
import load_tables.load_fact_tables as load_facts
import load_tables.load_dim_tables as load_dims
import producers.producer_fact_vuelo as producer_vuelo
import producers.producer_fact_transporte as producer_transporte
import producers.producer_fact_reserva as producer_reserva
import consumers.consumer_fact_vuelo as consumer_vuelo
import consumers.consumer_fact_transporte as consumer_transporte
import consumers.consumer_fact_reserva as consumer_reserva


def main():
    parser = argparse.ArgumentParser()
    options = parser.add_mutually_exclusive_group()

    options.add_argument('-f', '--fact', type=str, help= "Cargar tablas de hechos: ['vuelo', 'reserva', 'transporte', 'all']")
    options.add_argument('-d', '--dim', type=str, help= "Cargar tablas de dimensiones: ['aeropuerto', 'avion', 'ciudad', 'alojamiento', 'vehiculo', 'fecha', 'hora', 'all']")
    options.add_argument('-p', '--produce', type=str, help= "Iniciar flujo en tiempo real: ['vuelo', 'reserva', 'transporte']")
    options.add_argument('-c', '--consume', type=str, help= "Iniciar a consumir flujo de datos en tiempo real: ['vuelo', 'reserva', 'transporte']")
    options.add_argument('-t', '--tables', action='store_true', help="Crear tablas de dimensiones y hechos")
    options.add_argument('-dt', '--delete', action='store_true', help="Borrar todas las tablas")


    args = parser.parse_args()
    
    if args.fact != None:
        if args.fact == "vuelo":
            load_facts.load_flight_data("./data/infovuelos_sample.csv")
        elif args.fact == "transporte":
            load_facts.load_transport_data("./data/dim_vehiculo.csv")
        else:
            print(f"La tabla de hechos {args.fact} no existe")
    elif args.dim != None:
        if args.dim == "aeropuerto":
            load_dims.load_airport_data("./data/infovuelos_sample.csv")
        elif args.dim == "avion":
            load_dims.load_plane_data("./data/infovuelos_sample.csv")
        elif args.dim == "ciudad":
            load_dims.load_city_data("./data/dim_ciudad.csv")
        elif args.dim == "vehiculo":
            load_dims.load_vehicle_data("./data/dim_vehiculo.csv")
        elif args.dim == "alojamiento":
            load_dims.load_accomodation_data("./data/alojamientos_turisticos.csv")
        elif args.dim == "fecha":
            load_dims.load_date_data()
        elif args.dim == "hora":
            load_dims.load_hour_data()
        elif args.dim == "all":
            load_dims.load_city_data("./data/dim_ciudad.csv")
            load_dims.load_airport_data("./data/infovuelos_sample.csv")
            load_dims.load_plane_data("./data/infovuelos_sample.csv")
            load_dims.load_vehicle_data("./data/dim_vehiculo.csv")
            load_dims.load_accomodation_data("./data/alojamientos_turisticos.csv")
            load_dims.load_date_data()
            load_dims.load_hour_data()
        else:
            print(f"La dimensión {args.dim} no existe")
    elif args.produce != None:
        if args.produce == "vuelo":
            producer_vuelo.produce_live_data(100)
        elif args.produce == "transporte":
            producer_transporte.produce_live_data(100)
        elif args.produce == "reserva":
            producer_reserva.produce_live_data(100)
        else:
            print(f"No existen datos en tiempo real para {args.live}")
    elif args.consume != None:
        if args.consume == "vuelo":
            consumer_vuelo.consume_live_data()
        elif args.consume == "transporte":
            consumer_transporte.consume_live_data()
        elif args.consume == "reserva":
            consumer_reserva.consume_live_data()
        else:
            print(f"No existen datos en tiempo real para {args.live}")

    elif args.tables:
        print("Creando tablas...")
        try:
            create_tables.crear_tablas()
        except Exception as e:
            print(f"Hubo un error al crear las tablas: {e}")

    elif args.delete:
        print("Eliminando tablas...")
        try:
            delete_tables.borrar_tablas()
        except Exception as e:
            print(f"Hubo un error al eliminar las tablas: {e}")
    else:
        print("El argumento no es correcto")

    


if __name__ == "__main__":
    main()