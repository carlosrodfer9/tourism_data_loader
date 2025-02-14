# tourism_data_loader

## Modo de ejecucion

usage: main.py [-h] [-f FACT | -d DIM | -p PRODUCE | -c CONSUME]

optional arguments:
  -h, --help            show this help message and exit
  -f FACT, --fact FACT  Cargar tablas de hechos: ['vuelo', 'reserva', 'transporte', 'all']
  -d DIM, --dim DIM     Cargar tablas de dimensiones: ['aeropuerto', 'avion', 'ciudad', 'alojamiento', 'vehiculo', 'fecha', 'hora', 'all']
  -p PRODUCE, --produce PRODUCE
                        Iniciar flujo en tiempo real: ['vuelo', 'reserva', 'transporte']
  -c CONSUME, --consume CONSUME
                        Iniciar a consumir flujo de datos en tiempo real: ['vuelo', 'reserva', 'transporte']