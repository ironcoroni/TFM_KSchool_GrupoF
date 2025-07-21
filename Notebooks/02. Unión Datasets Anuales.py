import pandas as pd

# Unir los 4 datasets en un solo DataFrame sin pérdida de datos
archivos = [
    '2021_Accidentalidad_COMPLETO.csv',
    '2022_Accidentalidad_COMPLETO.csv',
    '2023_Accidentalidad_COMPLETO.csv',
    '2024_Accidentalidad_COMPLETO.csv'
]

dfs = [pd.read_csv(archivo, delimiter=',') for archivo in archivos]
df_unido = pd.concat(dfs, ignore_index=True)

# Comprobar el resultado
df_unido.info()
df_unido.head()


# Guardar el DataFrame unido en un archivo CSV
df_unido.to_csv('Accidentalidad_2021_2024_unido.csv', index=False)