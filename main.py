import argparse
import load_tables.load_fact_tables as load_facts
import load_tables.load_dim_tables as load_dims


def main():
    parser = argparse.ArgumentParser()
    options = parser.add_mutually_exclusive_group()

    options.add_argument('-f', '--fact', type=str, help= "Cargar tablas de hechos: ['vuelo', 'reserva', 'transporte', 'all']")
    options.add_argument('-d', '--dim', type=str, help= "Cargar tablas de dimensiones: ['aeropuerto', 'avion', 'ciudad', 'alojamiento', 'vehiculo', 'fecha', 'hora', 'all']")
    options.add_argument('-l', '--live', type=str, help= "Iniciar flujo en tiempo real: ['vuelo', 'reserva', 'transporte', 'all']")


    args = parser.parse_args()
    
    if args.fact != None:
        if args.fact == "vuelo":
            load_facts.load_flight_data("./data/infovuelos_sample.csv")
        else:
            print("nkscdm")
    elif args.dim != None:
        if args.dim == "aeropuerto":
            load_dims.load_airport_data("./data/infovuelos_sample.csv")
        elif args.dim == "avion":
            load_dims.load_plane_data("./data/infovuelos_sample.csv")
        elif args.dim == "ciudad":
            load_dims.load_city_data("./data/dim_ciudad.csv")
        elif args.dim == "vehiculo":
            load_dims.load_vehicle_data("./data/dim_vehiculo.csv")
    #elif args.live:
#
    else:
        print("El argumento no es correcto")


if __name__ == "__main__":
    main()