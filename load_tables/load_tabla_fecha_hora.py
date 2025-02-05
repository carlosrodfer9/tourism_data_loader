import psycopg2
from psycopg2 import sql
from datetime import date, timedelta

# Conexión a la base de datos
conn = psycopg2.connect(
    host="localhost",       
    dbname="postgres", # Nombre de la base de datos
    user="postgres",      # Tu usuario
    password="1234" # Tu contraseña
)

# Crear un cursor para ejecutar las consultas
cur = conn.cursor()

# Fechas de inicio y fin
fecha = date(2020, 1, 1)
fin_fecha = date(2030, 12, 31)

# Insertar datos
while fecha <= fin_fecha:
    id_fecha = int(fecha.year) * 10000 + int(fecha.month) * 100 + int(fecha.day)
    nombre_dia = fecha.strftime('%A')
    nombre_mes = fecha.strftime('%B')
    dia_semana = fecha.weekday()  # 0=lunes, 1=martes, ..., 6=domingo
    trimestre = (fecha.month - 1) // 3 + 1
    mes_trimestre = (fecha.month - 1) % 3 + 1

    cur.execute("""
        INSERT INTO Dim_Fecha (ID_Fecha, Dia, Mes, Año, NombreDia, NombreMes, DiaSemana, Trimestre, MesTrimestre)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (id_fecha, fecha.day, fecha.month, fecha.year, nombre_dia, nombre_mes, dia_semana, trimestre, mes_trimestre))
    
    # Incrementar un día
    fecha += timedelta(days=1)

# Confirmar los cambios en la base de datos
conn.commit()

# Insertar datos en Dim_Hora
for hora in range(24):
    for minuto in range(60):
        for segundo in range(60):
            am = hora < 12
            epoch = hora * 3600 + minuto * 60 + segundo
            id_hora = f"{hora:02d}{minuto:02d}{segundo:02d}"

            cur.execute("""
                INSERT INTO Dim_Hora (ID_Hora, Hora, Minuto, Segundo, AM, Epoch)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (id_hora, hora, minuto, segundo, am, epoch))

# Confirmar los cambios en la base de datos
conn.commit()


# Insertar datos en Dim_Ubicacion
for i in range(1, 101):
    latitud = 19.432608 + (i * 0.001)
    longitud = -99.133209 + (i * 0.001)
    cur.execute("""
        INSERT INTO dim_ubicacion (id_ubicacion, latitud, longitud, id_ciudad, ubicacion)
        VALUES (%s, %s, %s, %s, %s)
    """, (i, latitud, longitud, 1, 'prueba'))

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar el cursor y la conexión
cur.close()
conn.close()
