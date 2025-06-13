# visualizacion_proyecto.py

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def cargar_datos_limpios(ruta_csv):
    """
    Carga el CSV limpio generado por el ETL y prepara los tipos de datos.
    """
    print(f"📂 Cargando datos desde: {ruta_csv}")
    df = pd.read_csv(ruta_csv)

    # Convertir posibles columnas categóricas
    columnas_categoricas_conocidas = [
        'attrition', 'department', 'gender', 'educationfield', 'jobrole',
        'maritalstatus', 'businesstravel', 'overtime', 'over18', 'remotework'
    ]

    for col in columnas_categoricas_conocidas:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # Mostrar columnas detectadas
    num_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    cat_cols = df.select_dtypes(include='category').columns.tolist()
    print(f"📊 Columnas numéricas detectadas: {num_cols}")
    print(f"📋 Columnas categóricas detectadas: {cat_cols}")

    return df

def plot_all_histograms(df):
    num_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    for col in num_cols:
        plt.figure(figsize=(8, 4))
        sns.histplot(df[col].dropna(), kde=True, color='skyblue')
        plt.title(f"Distribución de {col}")
        plt.xlabel(col)
        plt.ylabel("Frecuencia")
        plt.tight_layout()
        plt.show()

def plot_all_barplots(df):
    cat_cols = df.select_dtypes(include='category').columns.tolist()
    for col in cat_cols:
        if df[col].nunique() < 20:
            plt.figure(figsize=(8, 4))
            sns.countplot(data=df, x=col, order=df[col].value_counts().index, palette='pastel')
            plt.title(f"Distribución de {col}")
            plt.xlabel(col)
            plt.ylabel("Conteo")
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()

def plot_all_piecharts(df):
    cat_cols = df.select_dtypes(include='category').columns.tolist()
    for col in cat_cols:
        if df[col].nunique() <= 6:
            plt.figure(figsize=(6, 6))
            df[col].value_counts().plot.pie(
                autopct='%1.1f%%', startangle=90, colors=sns.color_palette('pastel')
            )
            plt.ylabel('')
            plt.title(f"Distribución de {col}")
            plt.tight_layout()
            plt.show()




# %%
