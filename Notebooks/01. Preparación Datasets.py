import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from datetime import datetime
from meteostat import Point, Hourly
import os

# Ruta de los archivos originales
ruta_csv = r'C:\Users\Juan Mendoza\Juan\Kschool\Clases\TFM\Notebooks\Dataset originales'

# Función para clasificar la lluvia según mm/hora
def clasificar_lluvia(mm_por_hora):
    if pd.isna(mm_por_hora) or mm_por_hora == 0:
        return "Despejado"
    elif 0.1 <= mm_por_hora < 2:
        return "Lluvia débil"
    elif 2 <= mm_por_hora < 10:
        return "Lluvia moderada"
    elif 10 <= mm_por_hora < 50:
        return "Lluvia fuerte"
    else:
        return "Lluvia torrencial"

# Cargar datos meteorológicos clasificados (de meteostat)
madrid = Point(40.4168, -3.7038)
start = datetime(2021, 1, 1)
end = datetime(2024, 12, 31)
data = Hourly(madrid, start, end).fetch()
data['estado_meteorologico_clasificado'] = data['prcp'].apply(clasificar_lluvia)

# --- PROCESAMIENTO PARA CADA AÑO ---

columns_to_drop = [
    'numero', 'rango_edad', 'sexo',
    'cod_lesividad', 'lesividad',
    'positiva_alcohol', 'positiva_droga'
]
columnas = [
    'num_expediente', 'dia_hora', 'cod_distrito', 'distrito',
    'tipo_accidente', 'estado_meteorológico', 'tipo_vehiculo', 'tipo_persona'
]

for year in range(2021, 2025):
    print(f"\nProcesando año {year}...")
    file_path = os.path.join(ruta_csv, f'{year}_Accidentalidad.csv')
    df = pd.read_csv(file_path, delimiter=';')
    df = df.drop(columns=columns_to_drop)
    # Crear columna 'dia_hora'
    df['dia_hora'] = df['fecha'] + ' ' + df['hora']
    df['dia_hora'] = pd.to_datetime(df['dia_hora'], format='%d/%m/%Y %H:%M:%S').dt.floor('h')
    # Seleccionar columnas relevantes
    new_df = df[columnas]
    # Imputar estado meteorológico usando meteostat
    for index, row in new_df.iterrows():
        if pd.isna(row['estado_meteorológico']):
            fecha_hora_accidente = row['dia_hora']
            precipitacion = data['estado_meteorologico_clasificado'].get(fecha_hora_accidente, None)
            if precipitacion is not None:
                new_df.loc[index, 'estado_meteorológico'] = precipitacion
    # Imputar la moda si queda algún NaN
    new_df['estado_meteorológico'].fillna(new_df['estado_meteorológico'].mode()[0], inplace=True)
    # Exportar a CSV
    new_df.to_csv(f'{year}_Accidentalidad_COMPLETO.csv', index=False)
    print(f"Archivo {year}_Accidentalidad_COMPLETO.csv exportado correctamente.")

print("\nProcesamiento de años 2021-2024 completado.")