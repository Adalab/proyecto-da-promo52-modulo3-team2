# %%
import etl_funciones_proyecto as ef
import visualizacion_proyecto as vp

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
    ef.etl_process(
        nombre_archivo=archivo_entrada,
        nombre_tabla=nombre_tabla_mysql,
        archivo_limpio=archivo_limpio_salida,
        hacer_eda=hacer_eda
    )

# Ejecutar ETL y guardar el df limpio
df_limpio = ef.etl_process('HR RAW DATA.csv', 'datos_hr', 'HR_CLEAN_EDA.csv')

# Usar el df limpio para visualizaciones
vp.plot_all_histograms(df_limpio)
vp.plot_all_barplots(df_limpio)
vp.plot_all_piecharts(df_limpio)
# %%
