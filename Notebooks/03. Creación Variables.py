# 1. IMPORTACIÓN DE LIBRERÍAS NECESARIAS
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import itertools
import warnings
import re
warnings.filterwarnings('ignore')

# Función para corregir caracteres mal codificados
def corregir_encoding(texto):
    if not isinstance(texto, str):
        return texto
    
    # Si no hay coincidencias exactas, aplicamos reemplazos por partes
    texto = texto.replace('Ã±', 'ñ')
    texto = texto.replace('Ã¡', 'á')
    texto = texto.replace('Ã©', 'é')
    texto = texto.replace('Ã­', 'í')
    texto = texto.replace('Ã³', 'ó')
    texto = texto.replace('Ãº', 'ú')
    texto = texto.replace('Ã\x91', 'Ñ')
    texto = texto.replace('Ã\x81', 'Á')
    texto = texto.replace('Ã\x89', 'É')
    texto = texto.replace('Ã\x8d', 'Í')
    texto = texto.replace('Ã\x93', 'Ó')
    texto = texto.replace('Ã\x9a', 'Ú')
    
    return texto

# Función para corregir todas las columnas de texto en el DataFrame
def corregir_df(df):
    # Hacemos una copia para evitar problemas de modificación durante la iteración
    df_corregido = df.copy()
    
    # Aplicamos la corrección a todas las columnas de texto
    for columna in df_corregido.select_dtypes(include=['object']).columns:
        print(f"Corrigiendo columna: {columna}")
        df_corregido[columna] = df_corregido[columna].apply(corregir_encoding)
        
    return df_corregido

# 1. CARGA DE DATOS
# Cargamos el dataset procesado de accidentes de 2020 con la ruta completa

ruta_archivo = r'C:\Users\afono\Desktop\TFM - ALBERT FONOLLET TORRUBIANO\TFM_Kschool\Dataset utilizado por TFM_agregados_final (tras pipeline n2)\Accidentalidad_2021_2024_unido.csv'
print(f"Cargando archivo: {ruta_archivo}")

# Codificamos con latin-1
df_2020 = pd.read_csv(ruta_archivo, encoding='latin-1')

# Aplicamos la corrección de caracteres mal codificados
print("Corrigiendo caracteres mal codificados en el dataset inicial...")
df_2020 = corregir_df(df_2020)

# Corregimos también los nombres de las columnas
columnas_originales = df_2020.columns.tolist()
columnas_corregidas = [corregir_encoding(col) for col in columnas_originales]
df_2020.columns = columnas_corregidas

# Reemplazamos "Se desconoce" en estado_meteorológico por "Despejado"
if 'estado_meteorológico' in df_2020.columns:
    desconocidos = df_2020['estado_meteorológico'].isin(['Se desconoce', 'se desconoce', ''])
    df_2020.loc[desconocidos, 'estado_meteorológico'] = 'Despejado'

# Reemplazamos valores vacíos o 0 en tipo_vehiculo por "Turismo"
if 'tipo_vehiculo' in df_2020.columns:
    vacios = df_2020['tipo_vehiculo'].isin(['', '0', 0, 'Sin especificar', 'sin especificar']) | df_2020['tipo_vehiculo'].isna()
    df_2020.loc[vacios, 'tipo_vehiculo'] = 'Turismo'


# 2. AGREGACIÓN POR DISTRITO

# Contamos los accidentes por distrito
accidentes_por_distrito = df_2020.groupby('distrito')['num_expediente'].nunique().reset_index()
accidentes_por_distrito.columns = ['distrito', 'num_accidentes']

# Ordenamos por número de accidentes (descendente)
accidentes_por_distrito = accidentes_por_distrito.sort_values('num_accidentes', ascending=False)

# 3. CLASIFICACIÓN DE DISTRITOS POR NIVEL DE ACCIDENTALIDAD

# Dividimos en 4 grupos 
accidentes_por_distrito['grupo_distrito'] = pd.qcut(accidentes_por_distrito.index, 4, labels=['Alto', 'Medio-Alto', 'Medio-Bajo', 'Bajo'])

# Creamos un mapeo de distrito a grupo para aplicarlo al dataset original
mapeo_distritos = dict(zip(accidentes_por_distrito['distrito'], accidentes_por_distrito['grupo_distrito']))
df_2020['grupo_distrito'] = df_2020['distrito'].map(mapeo_distritos)

# 4. AGREGACIÓN TEMPORAL

# Extraemos el día de la semana de la columna de fecha. 
if 'dia_hora' in df_2020.columns:
    fecha_col = 'dia_hora'
    df_2020['dia_semana'] = pd.to_datetime(df_2020[fecha_col]).dt.day_name()
    
    # Traducir los nombres de los días al español
    traduccion = {
        'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles',
        'Thursday': 'Jueves', 'Friday': 'Viernes', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
    }
    df_2020['dia_semana'] = df_2020['dia_semana'].map(lambda x: traduccion.get(x, x))
    
    # Extraer la hora y crear la franja horaria
    df_2020['hora'] = pd.to_datetime(df_2020['dia_hora']).dt.hour
    df_2020['franja_horaria'] = df_2020['hora'].apply(lambda h: 
        'Mañana' if 6 <= h < 12 else
        'Tarde' if 12 <= h < 18 else
        'Noche' if 18 <= h < 22 else
        'Madrugada'
    )
    
    # Ahora podemos agrupar por día de la semana
    accidentes_por_dia = df_2020.groupby('dia_semana')['num_expediente'].nunique()
    # Reordenamos los días
    orden_dias = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    accidentes_por_dia = accidentes_por_dia.reindex(orden_dias)

    

# MOmento del día
if 'momento_dia' in df_2020.columns:
    accidentes_por_momento = df_2020.groupby('momento_dia')['num_expediente'].nunique()
    # Reordenamos si existen todas las categorías
    try:
        accidentes_por_momento = accidentes_por_momento.reindex(['Mañana', 'Tarde', 'Noche', 'Madrugada'])
    except:
        pass  # Si no existen todas las categorías, dejamos el orden original
else:
    accidentes_por_momento = pd.Series(df_2020['num_expediente'].nunique(), index=['Total'])

# 6. AGREGACIÓN POR TIPO DE ACCIDENTE

accidentes_por_tipo = df_2020.groupby('tipo_accidente')['num_expediente'].nunique().sort_values(ascending=False)

# 7. AGREGACIÓN POR CONDICIONES METEOROLÓGICAS

# Verificamos el nombre exacto de la columna meteorológica
columnas_meteo = [col for col in df_2020.columns if 'meteoro' in col.lower()]
if columnas_meteo:
    columna_meteo = columnas_meteo[0]
    print(f"Usando columna meteorológica: {columna_meteo}")
    accidentes_por_meteo = df_2020.groupby(columna_meteo)['num_expediente'].nunique().sort_values(ascending=False)
else:
    print("No se encontró columna meteorológica")
    accidentes_por_meteo = pd.Series(df_2020['num_expediente'].nunique(), index=['Total'])

# 8. FUNCIÓN PARA CALCULAR ÍNDICE DE GRAVEDAD

def calcular_indice_gravedad(row):
    
    # Base: todos los accidentes empiezan con un valor base establecido a 1
    gravedad = 1.0
    
    # Factor por tipo de personas involucradas
    if 'Peatones' in row and row['Peatones'] > 0:
        gravedad += 3.0  # Los accidentes con peatones son potencialmente más graves
    
    # Factor por tipo de vehículos
    if 'Vehículo de dos ruedas' in row and row['Vehículo de dos ruedas'] > 0:
        gravedad += 1.8  # Vehículos de dos ruedas aumentan la gravedad potencial
    if 'Vehículo pesado' in row and row['Vehículo pesado'] > 0:
        gravedad += 1.5  # Vehículos pesados aumentan la gravedad
        
    # Factor por número de implicados
    if 'total_implicados' in row:
        gravedad += (row['total_implicados'] - 1) * 0.1
    
    # Factor por diversidad de vehículos
    if 'diversidad_vehiculos' in row:
        gravedad += (row['diversidad_vehiculos'] - 1) * 0.15
    
    # Factor por tipo de accidente 
    if 'tipo_accidente' in row:
        tipo_acc = str(row['tipo_accidente']).lower()
        if 'atropello' in tipo_acc:
            if 'animal' in tipo_acc:
                gravedad += 1.0  # Atropello a animal es menos grave que a persona
            else:
                gravedad += 2.0  # Atropello a persona es muy grave
        elif 'colisión frontal' in tipo_acc:
            gravedad += 1.8  # Colisión frontal es muy grave
        elif 'colisión fronto-lateral' in tipo_acc or 'fronto' in tipo_acc:
            gravedad += 1.5  # Colisión fronto-lateral es grave
        elif 'colisión lateral' in tipo_acc:
            gravedad += 1.2  # Colisión lateral es moderadamente grave
        elif 'colisión múltiple' in tipo_acc or 'multiple' in tipo_acc:
            gravedad += 1.7  # Colisión múltiple suele ser grave
        elif 'alcance' in tipo_acc:
            gravedad += 1.0  # Alcance es menos grave que otras colisiones
        elif 'choque' in tipo_acc and 'obstáculo' in tipo_acc:
            gravedad += 1.3  # Choque contra obstáculo fijo
        elif 'vuelco' in tipo_acc:
            gravedad += 1.7  # Vuelco es bastante grave
        elif 'caída' in tipo_acc or 'caida' in tipo_acc:
            gravedad += 1.6  # Caída (especialmente para vehículos de dos ruedas)
        elif ('salida' in tipo_acc and 'vía' in tipo_acc) or 'solo salida' in tipo_acc:
            gravedad += 1.4  # Salida de vía
        elif 'despeñamiento' in tipo_acc or 'despen' in tipo_acc:
            gravedad += 1.9  # Despeñamiento es muy grave

    # Factor por hora del día 
    if 'dia_hora' in row:
        hora = pd.to_datetime(row['dia_hora']).hour
        if 0 <= hora < 6:  # Madrugada (mayor riesgo por fatiga y visibilidad)
            gravedad += 0.6
        elif 6 <= hora < 9:  # Hora punta mañana
            gravedad += 0.4
        elif 9 <= hora < 17:  # Horario laboral (menor riesgo)
            gravedad += 0.1
        elif 17 <= hora < 20:  # Hora punta tarde
            gravedad += 0.4
        elif 20 <= hora < 23:  # Noche
            gravedad += 0.5
    
    # Factor por día de la semana 
    if 'dia_semana' in row:
        dia = str(row['dia_semana']).lower()
        if 'viernes' in dia:
            gravedad += 0.2  # Viernes tarde/noche suele tener accidentes más graves
        elif 'sábado' in dia or 'sabado' in dia:
            gravedad += 0.3  # Fin de semana
        elif 'domingo' in dia:
            gravedad += 0.3  # Fin de semana
        elif 'lunes' in dia or 'lun' in dia:
            gravedad += 0.1  # Lunes tiene ligeramente más accidentes por fatiga post-fin de semana
    
    # Factor por estado meteorológico
    # Buscamos la columna meteorológica 
    columna_meteo = None
    for col in row.index:
        if 'meteoro' in str(col).lower():
            columna_meteo = col
            break
    
    if columna_meteo and columna_meteo in row:
        estado = str(row[columna_meteo]).lower()
        if estado == 'despejado':
            gravedad += 0.0  # Condiciones óptimas, no aumenta el riesgo
        elif 'lluvia débil' in estado or 'lluvia debil' in estado:  
            gravedad += 0.8  # Lluvia débil aumenta moderadamente el riesgo
        elif 'lluvia intensa' in estado or 'llubia intensa' in estado:  
            gravedad += 1.5  # Lluvia intensa aumenta significativamente el riesgo
        elif 'granizando' in estado or 'granizo' in estado:
            gravedad += 1.8  # Granizo es muy peligroso para la conducción
        elif 'nevando' in estado or 'nieve' in estado:
            gravedad += 2.0  # Nieve es extremadamente peligrosa
        elif 'nublado' in estado:
            gravedad += 0.3  # Nublado reduce ligeramente la visibilidad
            
    return round(gravedad, 2)

# 9. CREACIÓN DEL DATASET FINAL

# Primero, contamos los tipos de personas por expediente
conteo_personas = df_2020.groupby(['num_expediente', 'tipo_persona']).size().unstack(fill_value=0)


for tipo in ['Conductor', 'Pasajero', 'Peatón']:
    if tipo not in conteo_personas.columns:
        conteo_personas[tipo] = 0

# Renombramos las columnas para mayor claridad
conteo_personas.columns = ['Conductores', 'Pasajeros', 'Peatones']

# Contamos los tipos de vehículos por expediente
# Filtramos solo los conductores para contar vehículos (sin pasajeros y sin peatones)
df_vehiculos = df_2020[df_2020['tipo_persona'] == 'Conductor']
# Contamos cuántos vehículos de cada tipo hay en cada expediente
conteo_vehiculos = df_vehiculos.groupby(['num_expediente', 'tipo_vehiculo']).size().unstack(fill_value=0)

# Creamos categorías generales de vehículos con valores iniciales de 0
conteo_vehiculos['Vehículo de dos ruedas'] = 0
conteo_vehiculos['Vehículo pesado'] = 0
conteo_vehiculos['Otros vehículos'] = 0

# Asignamos directamente los valores de la columna "Turismo" original - Para evitar duplicidad de los datos
if "Turismo" in conteo_vehiculos.columns:
    # Guardamos los valores originales de la columna Turismo
    turismo_values = conteo_vehiculos["Turismo"].copy()
    # Creamos la columna de categoría
    conteo_vehiculos['Turismo'] = turismo_values


# Clasificamos los vehículos en categorías generales
for col in conteo_vehiculos.columns:
    col_lower = str(col).lower()
    # Saltamos las columnas de categorías generales
    if col in ['Vehículo de dos ruedas', 'Vehículo pesado', 'Turismo', 'Otros vehículos']:
        continue
    
    # Vehículos de dos ruedas
    if ('moto' in col_lower or 'ciclomotor' in col_lower or 'bicicleta' in col_lower or
        'ciclo' in col_lower or 'patinete' in col_lower or 'vmu' in col_lower or
        'epac' in col_lower or 'tres ruedas' in col_lower):
        conteo_vehiculos['Vehículo de dos ruedas'] += conteo_vehiculos[col]
    
    # Vehículos pesados
    elif ('camión' in col_lower or 'camion' in col_lower or 'autobús' in col_lower or 
          'autobus' in col_lower or 'emt' in col_lower or 'tractocamión' in col_lower or
          'articulado' in col_lower or 'remolque' in col_lower or 'semiremolque' in col_lower or
          'bomberos' in col_lower or 'ambulancia' in col_lower or 'maquinaria' in col_lower):
        conteo_vehiculos['Vehículo pesado'] += conteo_vehiculos[col]
    
    # Turismos (ya establecidos separadamente anteriormente, pero por si acaso)
    elif 'turismo' in col_lower or 'todo terreno' in col_lower or 'autocaravana' in col_lower:
        conteo_vehiculos['Turismo'] += conteo_vehiculos[col]
    
    # Otros vehículos
    else:
        conteo_vehiculos['Otros vehículos'] += conteo_vehiculos[col]


# Creamos un dataset base con un registro único por expediente
df_base = df_2020.drop_duplicates(subset=['num_expediente'])

# Unimos la información de personas y vehículos al dataset base
df_final = df_base.merge(conteo_personas, left_on='num_expediente', right_index=True, how='left')

# Seleccionamos las columnas de vehículos que hemos creado
columnas_vehiculos = [col for col in conteo_vehiculos.columns if col in ['Vehículo de dos ruedas', 'Vehículo pesado', 'Turismo', 'Otros vehículos']]
print(f"\nColumnas de vehículos seleccionadas: {columnas_vehiculos}")

# Realizamos la unión
df_final = df_final.merge(conteo_vehiculos[columnas_vehiculos], left_on='num_expediente', right_index=True, how='left')

# Rellenamos los NaN con 0
df_final = df_final.fillna(0)

# Calculamos métricas adicionales
df_final['total_implicados'] = df_final['Conductores'] + df_final['Pasajeros'] + df_final['Peatones']
df_final['tiene_vulnerables'] = (df_final['Peatones'] > 0).astype(int)
df_final['diversidad_vehiculos'] = ((df_final['Vehículo de dos ruedas'] > 0).astype(int) + 
                                   (df_final['Vehículo pesado'] > 0).astype(int) + 
                                   (df_final['Turismo'] > 0).astype(int) + 
                                   (df_final['Otros vehículos'] > 0).astype(int))

# 10. APLICACIÓN DEL ÍNDICE DE GRAVEDAD
# Calculamos el índice de gravedad para cada accidente
df_final['indice_gravedad'] = df_final.apply(calcular_indice_gravedad, axis=1)

# Creamos categorías de gravedad (bajo, medio, alto)
df_final['categoria_gravedad'] = pd.qcut(df_final['indice_gravedad'], 3, labels=['Bajo', 'Medio', 'Alto'])

# 11. ANÁLISIS DE GRAVEDAD POR DISTRITO

gravedad_por_distrito = df_final.groupby('distrito')['indice_gravedad'].mean().sort_values(ascending=False)

# 12. ANÁLISIS DE GRAVEDAD POR TIPO DE ACCIDENTE
gravedad_por_tipo = df_final.groupby('tipo_accidente')['indice_gravedad'].mean().sort_values(ascending=False)

# 13. ANÁLISIS DE GRAVEDAD POR HORA DEL DÍA
# Extraemos la hora del día
try:
    df_final['hora'] = pd.to_datetime(df_final['dia_hora']).dt.hour
    # Definimos franjas horarias
    def get_franja_horaria(hora):
        if 6 <= hora < 12:
            return 'Mañana'
        elif 12 <= hora < 18:
            return 'Tarde'
        elif 18 <= hora < 22:
            return 'Noche'
        else:
            return 'Madrugada'
    
    df_final['franja_horaria'] = df_final['hora'].apply(get_franja_horaria)
    gravedad_por_franja = df_final.groupby('franja_horaria')['indice_gravedad'].mean()
    # Reordenamos
    try:
        gravedad_por_franja = gravedad_por_franja.reindex(['Mañana', 'Tarde', 'Noche', 'Madrugada'])
    except:
        pass
    
except Exception as e:
    print(f"\nNo se pudo analizar por franja horaria: {e}")

# 14. GUARDADO DE RESULTADOS

# Definimos la ruta donde guardar los resultados - DIRECTAMENTE EN LA CARPETA DEL PROYECTO
ruta_resultados = r'C:\Users\afono\Desktop\TFM - ALBERT FONOLLET TORRUBIANO\TFM_Kschool\resultados'

# Aplicamos la corrección de caracteres mal codificados a los resultados finales

# Función para corregir todo el DataFrame una vez más
def corregir_df_final(df):
    # Convertir todo el DataFrame a string para procesamiento
    df_str = df.astype(str)
    
    # Reemplazar directamente en todo el DataFrame
    df_corregido = df_str.replace({
        'MaÃ±ana': 'Mañana',
        'ColisiÃ³n lateral': 'Colisión lateral',
        'ColisiÃ³n fronto-lateral': 'Colisión fronto-lateral',
        'ColisiÃ³n frontal': 'Colisión frontal',
        'Choque contra obstÃ¡culo fijo': 'Choque contra obstáculo fijo',
        'Solo salida de la vÃ­a': 'Solo salida de la vía',
        'MiÃ©rcoles': 'Miércoles',
        'CHAMBERÃ': 'CHAMBERÍ',
        'CHAMARTÃN': 'CHAMARTÍN',
        'CaÃ­da': 'Caída',
        'CamiÃ³n rÃ­gido': 'Camión rígido',
        'Ã±': 'ñ',
        'Ã¡': 'á',
        'Ã©': 'é',
        'Ã­': 'í',
        'Ã³': 'ó',
        'Ãº': 'ú'
    }, regex=True)
    
    # Restaurar los tipos de datos originales donde sea posible
    for col in df.columns:
        if df[col].dtype != 'object':
            try:
                df_corregido[col] = df_corregido[col].astype(df[col].dtype)
            except:
                pass
    
    return df_corregido

# Aplicar la corrección directa
df_final = corregir_df_final(df_final)

# Seleccionamos solo las columnas relevantes para el modelo de predicción
columnas_relevantes = [
    'num_expediente',          # Identificador único
    'dia_hora',                # Fecha y hora del accidente
    'distrito',                # Ubicación geográfica
    'tipo_accidente',          # Tipo de accidente
    'estado_meteorológico',    # Condiciones meteorológicas
    'Conductores',             # Número de conductores implicados
    'Pasajeros',               # Número de pasajeros implicados
    'Peatones',                # Número de peatones implicados
    'Vehículo de dos ruedas',  # Presencia de vehículos de dos ruedas
    'Vehículo pesado',         # Presencia de vehículos pesados
    'Turismo',                 # Presencia de turismos
    'Otros vehículos',         # Presencia de otros vehículos
    'total_implicados',        # Total de personas implicadas
    'tiene_vulnerables',       # Indicador de presencia de usuarios vulnerables
    'diversidad_vehiculos',    # Diversidad de tipos de vehículos
    'franja_horaria',          # Franja horaria del día
    'indice_gravedad',         # Índice de gravedad calculado
    'categoria_gravedad'       # Categoría de gravedad (Bajo, Medio, Alto)
]

# Verificamos que todas las columnas existan en el DataFrame
columnas_existentes = [col for col in columnas_relevantes if col in df_final.columns]
print(f"Columnas seleccionadas: {len(columnas_existentes)} de {len(columnas_relevantes)}")
if len(columnas_existentes) < len(columnas_relevantes):
    columnas_faltantes = set(columnas_relevantes) - set(columnas_existentes)
    print(f"Columnas no encontradas: {columnas_faltantes}")

# Seleccionamos solo las columnas existentes
df_final_limpio = df_final[columnas_existentes]
print(f"Dimensiones del dataset final: {df_final_limpio.shape}")

# Guardamos el dataset final en formato Excel
print("\nGuardando dataset final en formato Excel...")
df_final_limpio.to_excel(os.path.join(ruta_resultados, 'df_2020_final_con_gravedad.xlsx'), index=False)
print(f"Dataset final guardado en: {os.path.join(ruta_resultados, 'df_2020_final_con_gravedad.xlsx')}")

# Guardamos las agregaciones principales en un solo archivo Excel con múltiples hojas
with pd.ExcelWriter(os.path.join(ruta_resultados, 'agregaciones.xlsx')) as writer:
    accidentes_por_distrito.to_excel(writer, sheet_name='Agregados_Distrito', index=False)
    gravedad_por_distrito.to_frame().to_excel(writer, sheet_name='Gravedad_Distrito')
    gravedad_por_tipo.to_frame().to_excel(writer, sheet_name='Gravedad_Tipo')
    
    # Guardamos también la agregación por franja horaria
    if 'gravedad_por_franja' in locals():
        # Aseguramos que los índices (nombres de franjas) estén correctamente codificados
        gravedad_por_franja.index = gravedad_por_franja.index.map(lambda x: corregir_encoding(x) if isinstance(x, str) else x)
        gravedad_por_franja.to_frame().to_excel(writer, sheet_name='Gravedad_Franja')

print(f"\nTodos los archivos guardados en formato Excel en: {ruta_resultados}")

# 15. GENERACIÓN DE DATOS DE NO ACCIDENTES PARA EL MODELO DE PREDICCIÓN

def generar_no_accidentes(df_accidentes, factor_multiplicador=5):
    
    #Generamos datos sintéticos de no accidentes basados en los datos de accidentes existentes

    # Creamos un DataFrame vacío para los no accidentes
    no_accidentes = []
    
    # Obtenemos distribuciones de variables clave
    distritos = df_accidentes['distrito'].value_counts(normalize=True)
    franjas_horarias = df_accidentes['franja_horaria'].value_counts(normalize=True) if 'franja_horaria' in df_accidentes.columns else None
    estados_meteo = df_accidentes['estado_meteorológico'].value_counts(normalize=True)
    
    # Generamos registros de no accidentes
    num_no_accidentes = len(df_accidentes) * factor_multiplicador
    print(f"Generando {num_no_accidentes} registros de no accidentes...")
    
    for i in range(num_no_accidentes):
        # Generamos un registro con distribuciones similares pero variaciones aleatorias
        distrito = np.random.choice(distritos.index, p=distritos.values)
        
        if franjas_horarias is not None:
            franja = np.random.choice(franjas_horarias.index, p=franjas_horarias.values)
        else:
            # Si no hay franjas horarias, generamos una aleatoria
            franja = np.random.choice(['Mañana', 'Tarde', 'Noche', 'Madrugada'])
            
        estado_meteo = np.random.choice(estados_meteo.index, p=estados_meteo.values)
        
        # Generamos una fecha/hora aleatoria dentro del mismo rango que los datos originales
        fecha_min = pd.to_datetime(df_accidentes['dia_hora']).min()
        fecha_max = pd.to_datetime(df_accidentes['dia_hora']).max()
        dias_rango = (fecha_max - fecha_min).days
        fecha_aleatoria = fecha_min + pd.Timedelta(days=np.random.randint(0, dias_rango))
        
        # Ajustamos la hora según la franja horaria
        if franja == 'Mañana':
            hora = np.random.randint(6, 12)
        elif franja == 'Tarde':
            hora = np.random.randint(12, 18)
        elif franja == 'Noche':
            hora = np.random.randint(18, 22)
        else:  # Madrugada
            hora = np.random.randint(22, 24) if np.random.random() < 0.5 else np.random.randint(0, 6)
        
        fecha_aleatoria = fecha_aleatoria.replace(hour=hora, minute=np.random.randint(0, 60))
        
        # Creamos el registro
        registro = {
            'num_expediente': f'NA{i:06d}',  # NA = No Accidente
            'dia_hora': fecha_aleatoria,
            'distrito': distrito,
            'estado_meteorológico': estado_meteo,
            'Conductores': np.random.randint(1, 4),  # Simulamos tráfico normal
            'Pasajeros': np.random.randint(0, 3),
            'Peatones': np.random.randint(0, 2),
            'Vehículo de dos ruedas': np.random.binomial(1, 0.3),  # 30% de probabilidad
            'Vehículo pesado': np.random.binomial(1, 0.1),  # 10% de probabilidad
            'Turismo': np.random.binomial(1, 0.8),  # 80% de probabilidad
            'Otros vehículos': np.random.binomial(1, 0.05),  # 5% de probabilidad
        }
        
        # Calculamos métricas adicionales
        registro['total_implicados'] = registro['Conductores'] + registro['Pasajeros'] + registro['Peatones']
        registro['tiene_vulnerables'] = 1 if registro['Peatones'] > 0 else 0
        registro['diversidad_vehiculos'] = (registro['Vehículo de dos ruedas'] + 
                                           registro['Vehículo pesado'] + 
                                           registro['Turismo'] + 
                                           registro['Otros vehículos'])
        registro['franja_horaria'] = franja
        
        no_accidentes.append(registro)
    
    # Convertimos a DataFrame
    df_no_accidentes = pd.DataFrame(no_accidentes)
    
    # Añadimos columnas necesarias para compatibilidad
    if 'indice_gravedad' in df_accidentes.columns:
        df_no_accidentes['indice_gravedad'] = 0  # No hay gravedad en los no-accidentes
    if 'categoria_gravedad' in df_accidentes.columns:
        df_no_accidentes['categoria_gravedad'] = 'Ninguno'  # No hay categoría de gravedad
        
    return df_no_accidentes

# Después de guardar el dataset final de accidentes
print("\nPreparando dataset para modelo de predicción...")

# Añadimos la etiqueta de accidente a los datos originales
df_final_con_etiqueta = df_final_limpio.copy()
df_final_con_etiqueta['es_accidente'] = 1

# Generamos datos de no accidentes (factor 5 para representar mejor la realidad, tambien previamente definido)
df_no_accidentes = generar_no_accidentes(df_final_con_etiqueta, factor_multiplicador=3)
df_no_accidentes['es_accidente'] = 0  # Variable para no accidentes

# Combinamos ambos datasets
df_modelo_completo = pd.concat([df_final_con_etiqueta, df_no_accidentes], ignore_index=True)

# Guardamos el dataset combinado para el modelo
print("\nGuardando dataset para modelo de predicción...")
df_modelo_completo.to_excel(os.path.join(ruta_resultados, 'dataset_modelo_prediccion.xlsx'), index=False)
print(f"Dataset para modelo guardado en: {os.path.join(ruta_resultados, 'dataset_modelo_prediccion.xlsx')}")

# Estadísticas finales del dataset
num_accidentes = df_modelo_completo['es_accidente'].sum()
num_no_accidentes = len(df_modelo_completo) - num_accidentes
print(f"\nEstadísticas del dataset para el modelo:")
print(f"  - Total de registros: {len(df_modelo_completo)}")
print(f"  - Accidentes: {num_accidentes} ({num_accidentes/len(df_modelo_completo)*100:.1f}%)")
print(f"  - No accidentes: {num_no_accidentes} ({num_no_accidentes/len(df_modelo_completo)*100:.1f}%)")

print("\n=== Procesamiento completado con éxito ===")


