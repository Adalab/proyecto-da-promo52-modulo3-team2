import pandas as pd
import pymysql
from sqlalchemy import create_engine 
import matplotlib.pyplot as plt 

# Configuración de la base de datos
host = '127.0.0.1'
user = 'root'
password = 'AlumnaAdalab'
database = 'bd_proyecto3ETL'

# =====================
# EXTRACT: Obtener los datos desde los archivos CSV
# =====================

def extract_data(nombre_archivo):
    """
    Carga un archivo CSV y lo convierte en DataFrame.
    """
    try:
        df = pd.read_csv(nombre_archivo, on_bad_lines='skip', index_col=0)
        print("✅ Datos extraídos correctamente.")
        return df
    except FileNotFoundError:
        print(f"❌ Error: El archivo '{nombre_archivo}' no se encontró.")
    except pd.errors.ParserError:
        print(f"❌ Error al parsear el archivo '{nombre_archivo}'.")
    return None


def eda_basico(df):
    
    '''
    Analiza info del df
    '''

    print('🌷Ejemplo de datos del DF:')
    print(df.head(3))
    print(df.tail(3))
    print(df.sample(3))
    print('________________________________________________________________________________________________________')

    print('🌻Número de Filas:')
    print(df.shape[0])
    print('________________________________________________________________________________________________________')

    print('🌱Número de Columnas:')
    print(df.shape[1])
    print('________________________________________________________________________________________________________')

    print('🌼Información de la tabla:')
    print(df.info())
    print('________________________________________________________________________________________________________')

    print('🌑Nombre de las columnas:')
    print(df.columns)
    print('________________________________________________________________________________________________________')

    print('🍄Descripción de los datos numéricos:')
    print(df.describe().T)
    print('________________________________________________________________________________________________________')

    print('🌋Descripción de los datos no-numéricos:')
    print(df.describe(include='object').T)
    print('________________________________________________________________________________________________________')

    print('🍂Saber si hay datos únicos:')
    print(df.nunique())
    print('________________________________________________________________________________________________________')

    print('🐖Que datos son nulos por columnas:')
    print(df.isnull().sum())
    print('________________________________________________________________________________________________________')

    print('🐲Filas duplicadas:')
    total_duplicados = df.duplicated().sum()
    if total_duplicados > 0:
        print(f'cantidad de duplicados: {total_duplicados}')
        print('Primeros duplicados')
        print(df[df.duplicated()].head(3))
    else:
        print('No hay duplicados')
    print('________________________________________________________________________________________________________')

    print('🪹 Columnas constantes (solo 1 valor único):')
    constantes = df.columns[df.nunique() <= 1]
    if len(constantes) > 0:
        print(f'{len(constantes)} columnas con 1 valor único:')
        print(constantes)
    else:
        print('No hay columnas constantes')
    print('________________________________________________________________________________________________________')
    
    print('🚀 Valores únicos en columnas categóricas:')
    for col in df.select_dtypes(include='object'):
        print(f'🔸 {col}')
        print('-----------------------------')
        print(df[col].unique())
        print('________________________________________________________________________________________________________')

    print('🧬 Tipos de datos por columna:')
    print(df.dtypes.value_counts())
    print('________________________________________________________________________________________________________')

    return df

# =====================
# TRANSFORM: Limpia, transformar los datos y visualiza histogramas
# =====================

def transform_data(df):
    """
    Limpieza básica: nulos, duplicados, minúsculas en nombres.
    """
    print("🧼 Transformando datos...")
    initial_shape = df.shape
    #df = df.dropna()
    df = df.drop_duplicates()
    df.columns = df.columns.str.strip().str.lower()
    print(f"✅ Datos limpios: {initial_shape} ➝ {df.shape}")
    return df



def histogramas_variables_num(df, bins=30):
    """
    Genera histogramas para todas las variables numéricas en el DataFrame.

    Parámetros:
    - df: pandas.DataFrame con datos procesados.
    - bins: número de barras en cada histograma (por defecto 30).
    """
    # Seleccionar columnas numéricas
    columnas_num = df.select_dtypes(include=['number']).columns

    for columna in columnas_num:
        plt.figure(figsize=(8, 5))
        plt.hist(df[columna].dropna(), bins=bins, edgecolor='black')
        plt.title(f'Histograma de {columna}')
        plt.xlabel(columna)
        plt.ylabel('Frecuencia')
        plt.grid(True)
        plt.tight_layout()
        plt.show()


# =====================
# SAVE: Guardar archivo limpio
# =====================

def guardar(df, nombre_archivo): 
    """
    Guarda el DataFrame limpio como CSV.
    """
    try:
        df.to_csv(nombre_archivo, index=False)
        print(f"💾 Archivo guardado como '{nombre_archivo}'.")
    except Exception as e:
        print(f"❌ Error al guardar el archivo: {e}")


# =====================
# LOAD: Crear la base de datos y cargar los datos
# =====================

def create_db():
    """
    Crea la base de datos en MySQL si no existe.
    """
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password
        )
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        connection.close()
        print("🛠️ Base de Datos verificada/creada exitosamente.")
    except Exception as e:
        print(f"❌ Error creando la base de datos: {e}")


def load_data(table_name, df_clean):
    """
    Carga los datos del DataFrame a una tabla en MySQL.
    """
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        df_clean.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"📥 Datos insertados en la tabla '{table_name}' correctamente.")
    except Exception as e:
        print(f"❌ Error al cargar los datos: {e}")

# =====================
# Proceso ETL completo
# =====================

def etl_process(nombre_archivo, nombre_tabla, archivo_limpio='archivo_limpio.csv', hacer_eda=False):
    """
    Ejecuta el proceso completo de ETL.
    """
    print("🚀 Iniciando proceso ETL...")
    create_db()

    df = extract_data(nombre_archivo)
    if df is not None:
        if hacer_eda:
            eda_basico(df)
        
        df_clean = transform_data(df)
        guardar(df_clean, archivo_limpio)
        load_data(nombre_tabla, df_clean)
    print("✅ Proceso ETL completado.") 


