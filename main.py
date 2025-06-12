import etl_funciones

if __name__ == "__main__":
    # === Parámetros personalizables ===
    archivo_entrada = 'HR RAW DATA.csv'           # CSV original
    nombre_tabla_mysql = 'hr_raw_data'            # Nombre de tabla en MySQL
    archivo_limpio_salida = 'HR_CLEAN_EDA.csv'    # CSV limpio
    hacer_eda = True                               # Cambiar a False si no quieres análisis exploratorio

    # Configuración de la base de datos
    host = '127.0.0.1'
    user = 'root'
    password = 'AlumnaAdalab'
    database = 'bd_proyecto3ETL'

    # === Ejecutar el proceso ETL ===
    etl_funciones.etl_process(
        nombre_archivo=archivo_entrada,
        nombre_tabla=nombre_tabla_mysql,
        archivo_limpio=archivo_limpio_salida,
        hacer_eda=hacer_eda
    )


# ejecutar el script principal: python main.py