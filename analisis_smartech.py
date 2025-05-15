import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# --- CONFIGURACIÓN ---
HOY = datetime.now()
CARPETA_SALIDA = "output"

# Crear carpeta de salida si no existe
if not os.path.exists(CARPETA_SALIDA):
    os.makedirs(CARPETA_SALIDA)

# --- PASO 0: GENERACIÓN DE DATOS SIMULADOS ---
def generar_datos_clientes(num_filas=35):
    print("Generando datos de clientes...")
    data_clientes = {
        'Cliente_ID': [f'C{str(i).zfill(3)}' for i in range(num_filas - 5)] + [f'C{str(i).zfill(3)}' for i in range(5)], # Duplicados
        'Nombre': [f'Cliente_{i}' for i in range(num_filas)],
        'Edad': np.random.randint(18, 70, size=num_filas).astype(float),
        'Género': np.random.choice(['M', 'F', np.nan], size=num_filas, p=[0.45, 0.45, 0.1]), # Valores nulos
        'Ciudad': np.random.choice(['Lima', 'Arequipa', 'Trujillo', 'Cusco', 'Piura', 'Chiclayo', 'Iquitos', np.nan], size=num_filas, p=[0.2, 0.15, 0.15, 0.1, 0.1, 0.1, 0.1, 0.1]), # Nulos en Ciudad
        'Fecha_Registro': [(HOY - timedelta(days=np.random.randint(30, 1000))).strftime('%Y-%m-%d %H:%M:%S') for _ in range(num_filas)]
    }
    clientes_df = pd.DataFrame(data_clientes)
    clientes_df.loc[np.random.choice(clientes_df.index, size=int(num_filas*0.05), replace=False), 'Edad'] = None # Más valores faltantes
    clientes_df.loc[clientes_df.sample(n=2).index, 'Edad'] = [150, 5] # Outliers
    # Introducir una fila casi toda nula
    clientes_df.loc[num_filas] = {'Cliente_ID': f'C{str(num_filas).zfill(3)}', 'Nombre':None, 'Edad':None, 'Género':None, 'Ciudad':None, 'Fecha_Registro':None}
    print(f"  {len(clientes_df)} filas generadas para clientes.")
    return clientes_df

def generar_datos_compras(clientes_df, num_filas=45):
    print("Generando datos de compras...")
    clientes_existentes = clientes_df['Cliente_ID'].dropna().unique()
    cliente_ids_compras = np.random.choice(clientes_existentes, size=num_filas - 10).tolist() + \
                          [f'C_NO_EXISTE_{str(i).zfill(3)}' for i in range(3)] + \
                          [None] * 2 + \
                          [clientes_existentes[0]]*5 # Clientes que no existen, nulos y duplicados de compra

    data_compras = {
        'Compra_ID': [f'CP{str(i).zfill(3)}' for i in range(num_filas - 3)] + [f'CP{str(i).zfill(3)}' for i in range(3)], # Duplicados
        'Cliente_ID': cliente_ids_compras,
        'Producto': np.random.choice(['Laptop', 'Mouse', 'Teclado', 'Monitor', 'Tablet', 'Celular', 'Audífonos', 'Impresora'], size=num_filas),
        'Categoría': '',
        'Precio': np.random.uniform(20, 2500, size=num_filas).astype(float),
        'Cantidad': np.random.randint(1, 5, size=num_filas),
        'Fecha_Compra': [(HOY - timedelta(days=np.random.randint(1, 700))).strftime('%Y-%m-%d %H:%M:%S') if np.random.rand() > 0.05 else None for _ in range(num_filas)] # Nulos en Fecha_Compra
    }
    compras_df = pd.DataFrame(data_compras)
    map_categoria = {
        'Laptop': 'Electrónicos', 'Mouse': 'Accesorios', 'Teclado': 'Accesorios',
        'Monitor': 'Electrónicos', 'Tablet': 'Electrónicos', 'Celular': 'Electrónicos',
        'Audífonos': 'Accesorios', 'Impresora': 'Oficina'
    }
    compras_df['Categoría'] = compras_df['Producto'].map(map_categoria)
    compras_df.loc[np.random.choice(compras_df.index, size=int(num_filas*0.08), replace=False), 'Categoría'] = np.nan # Nulos en Categoría

    compras_df.loc[compras_df.sample(n=1).index, 'Precio'] = 10000 # Outlier
    compras_df.loc[np.random.choice(compras_df.index, size=int(num_filas*0.05), replace=False), 'Precio'] = None # Faltantes
    compras_df.loc[compras_df.sample(n=1).index, 'Cantidad'] = 50 # Outlier
    print(f"  {len(compras_df)} filas generadas para compras.")
    return compras_df

def generar_datos_navegacion(clientes_df, num_filas=55): # Aumentado para más duplicados y nulos
    print("Generando datos de navegación...")
    clientes_existentes = clientes_df['Cliente_ID'].dropna().unique()
    cliente_ids_navegacion = np.random.choice(clientes_existentes, size=num_filas - 10).tolist() + \
                             [clientes_existentes[i % len(clientes_existentes)] for i in range(5)] + \
                             [None] * 5 # Duplicados de sesión y Cliente_ID nulos

    data_navegacion = {
        'Cliente_ID': cliente_ids_navegacion,
        'Fecha': [(HOY - timedelta(days=np.random.randint(1, 365))).strftime('%Y-%m-%d %H:%M:%S') if np.random.rand() > 0.05 else None for _ in range(num_filas)],
        'Tiempo_Sesion(min)': np.random.uniform(1, 180, size=num_filas).astype(float),
        'Clicks': np.random.randint(1, 200, size=num_filas).astype(float), # Permitir float para NaN
        'Visitó_Carrito': np.random.choice([True, False, np.nan], size=num_filas, p=[0.5, 0.3, 0.2]), # Más nulos
        'Completó_Compra': np.random.choice([True, False], size=num_filas)
    }
    navegacion_df = pd.DataFrame(data_navegacion)
    navegacion_df.loc[np.random.choice(navegacion_df.index, size=int(num_filas*0.07), replace=False), 'Tiempo_Sesion(min)'] = None # Faltantes
    navegacion_df.loc[navegacion_df.sample(n=2).index, 'Tiempo_Sesion(min)'] = [500, 0.1] # Outliers
    navegacion_df.loc[np.random.choice(navegacion_df.index, size=int(num_filas*0.07), replace=False), 'Clicks'] = None # Faltantes
    navegacion_df.loc[navegacion_df.sample(n=1).index, 'Clicks'] = 1000 # Outlier
    navegacion_df.loc[navegacion_df['Visitó_Carrito'] == False, 'Completó_Compra'] = False # Coherencia
    # Introducir algunas filas completamente duplicadas
    navegacion_df = pd.concat([navegacion_df, navegacion_df.sample(n=5, random_state=1)], ignore_index=True)
    print(f"  {len(navegacion_df)} filas generadas para navegación.")
    return navegacion_df

# --- PASO 1: CARGA Y REVISIÓN DE LOS DATOS ---
def revisar_dataframe(df, nombre_df):
    print(f"\n--- Revisión del DataFrame: {nombre_df} ---")
    print(f"Dimensiones: {df.shape}")
    print("\nPrimeras 5 filas:")
    print(df.head())
    print("\nInformación general (tipos de datos, nulos):")
    df.info()

    # Valores nulos (más explícito que info)
    nulos_por_columna = df.isnull().sum()
    print(f"\nCantidad de valores NULOS por columna:\n{nulos_por_columna[nulos_por_columna > 0]}")
    total_nulos = nulos_por_columna.sum()
    print(f"Total de valores NULOS en el DataFrame: {total_nulos}")

    # Valores faltantes (consideramos nulos como faltantes aquí)
    faltantes_por_columna = nulos_por_columna
    print(f"\nCantidad de valores FALTANTES (identificados como nulos) por columna:\n{faltantes_por_columna[faltantes_por_columna > 0]}")
    total_faltantes = total_nulos
    print(f"Total de valores FALTANTES en el DataFrame: {total_faltantes}")

    # Valores duplicados (filas completas)
    duplicados = df.duplicated().sum()
    print(f"\nCantidad de filas DUPLICADAS: {duplicados}")

    # Outliers (ejemplo simple usando el método del Rango Intercuartílico - IQR)
    print("\nIdentificación de OUTLIERS (método IQR para columnas numéricas):")
    outliers_detectados = {}
    numeric_cols = df.select_dtypes(include=np.number).columns
    if not numeric_cols.empty:
        for column in numeric_cols:
            if df[column].isnull().all(): # Skip if all values are NaN
                print(f"  Columna '{column}': Todos los valores son NaN, se omite detección de outliers.")
                continue
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            if IQR == 0 : # Avoid division by zero or issues if all values are the same (after some cleaning)
                print(f"  Columna '{column}': IQR es 0, no se pueden detectar outliers con este método aquí.")
                continue
            limite_inferior = Q1 - 1.5 * IQR
            limite_superior = Q3 + 1.5 * IQR
            
            outliers_col = df[(df[column] < limite_inferior) | (df[column] > limite_superior)]
            if not outliers_col.empty:
                outliers_detectados[column] = len(outliers_col)
        if outliers_detectados:
            for col, num in outliers_detectados.items():
                 print(f"  Columna '{col}': {num} outliers detectados.")
        else:
            print("  No se detectaron outliers significativos con el método IQR en columnas numéricas.")
    else:
        print("  No hay columnas numéricas para la detección de outliers.")
    
    total_outliers_estimados = sum(outliers_detectados.values())
    print(f"Total de OUTLIERS (estimados por IQR en columnas numéricas): {total_outliers_estimados}")
    return {"nulos": total_nulos, "faltantes": total_faltantes, "duplicados": duplicados, "outliers_estimados": total_outliers_estimados}

# --- PASO 2: LIMPIEZA DE DATOS ---
def limpiar_datos(clientes_df, compras_df, navegacion_df):
    print("\n--- 2. Limpieza de datos ---")

    # Clientes
    print("\nLimpiando Clientes DF:")
    clientes_df.drop_duplicates(inplace=True)
    print(f"  Filas después de drop_duplicates: {len(clientes_df)}")
    clientes_df.dropna(subset=['Cliente_ID', 'Fecha_Registro'], inplace=True) # Columnas críticas
    print(f"  Filas después de dropna en Cliente_ID, Fecha_Registro: {len(clientes_df)}")
    # Outliers Edad (Cap)
    if 'Edad' in clientes_df.columns and not clientes_df['Edad'].isnull().all():
        edad_q1 = clientes_df['Edad'].quantile(0.25)
        edad_q3 = clientes_df['Edad'].quantile(0.75)
        edad_iqr = edad_q3 - edad_q1
        if edad_iqr > 0:
            edad_lim_inf = edad_q1 - 1.5 * edad_iqr
            edad_lim_sup = edad_q3 + 1.5 * edad_iqr
            clientes_df['Edad'] = np.where(clientes_df['Edad'] < edad_lim_inf, edad_lim_inf, clientes_df['Edad'])
            clientes_df['Edad'] = np.where(clientes_df['Edad'] > edad_lim_sup, edad_lim_sup, clientes_df['Edad'])
    clientes_df['Edad'].fillna(clientes_df['Edad'].median(), inplace=True)
    clientes_df['Género'].fillna(clientes_df['Género'].mode()[0] if not clientes_df['Género'].mode().empty else 'Desconocido', inplace=True)
    clientes_df['Ciudad'].fillna('Desconocida', inplace=True)

    # Compras
    print("\nLimpiando Compras DF:")
    compras_df.drop_duplicates(inplace=True)
    print(f"  Filas después de drop_duplicates: {len(compras_df)}")
    compras_df.dropna(subset=['Compra_ID', 'Cliente_ID', 'Fecha_Compra', 'Producto'], inplace=True) # Críticas
    print(f"  Filas después de dropna en columnas críticas: {len(compras_df)}")
    # Outliers Precio (Cap)
    if 'Precio' in compras_df.columns and not compras_df['Precio'].isnull().all():
        precio_q1 = compras_df['Precio'].quantile(0.25)
        precio_q3 = compras_df['Precio'].quantile(0.75)
        precio_iqr = precio_q3 - precio_q1
        if precio_iqr > 0:
            precio_lim_inf = precio_q1 - 1.5 * precio_iqr
            precio_lim_sup = precio_q3 + 1.5 * precio_iqr
            compras_df['Precio'] = np.where(compras_df['Precio'] < precio_lim_inf, precio_lim_inf, compras_df['Precio'])
            compras_df['Precio'] = np.where(compras_df['Precio'] > precio_lim_sup, precio_lim_sup, compras_df['Precio'])
    compras_df['Precio'].fillna(compras_df['Precio'].median(), inplace=True)
    compras_df['Categoría'].fillna('Otros', inplace=True)
    # Outliers Cantidad (Cap) - puede ser más agresivo, o revisar lógica de negocio
    if 'Cantidad' in compras_df.columns and not compras_df['Cantidad'].isnull().all():
        cant_q1 = compras_df['Cantidad'].quantile(0.25)
        cant_q3 = compras_df['Cantidad'].quantile(0.75)
        cant_iqr = cant_q3 - cant_q1
        if cant_iqr > 0 :
            cant_lim_sup = cant_q3 + 1.5 * cant_iqr
            compras_df['Cantidad'] = np.where(compras_df['Cantidad'] > cant_lim_sup, cant_lim_sup, compras_df['Cantidad'])
    compras_df['Cantidad'].fillna(compras_df['Cantidad'].median(), inplace=True)


    # Navegación
    print("\nLimpiando Navegación DF:")
    navegacion_df.drop_duplicates(inplace=True)
    print(f"  Filas después de drop_duplicates: {len(navegacion_df)}")
    navegacion_df.dropna(subset=['Cliente_ID', 'Fecha'], inplace=True) # Críticas
    print(f"  Filas después de dropna en Cliente_ID, Fecha: {len(navegacion_df)}")
    # Outliers Tiempo_Sesion(min) (Cap)
    if 'Tiempo_Sesion(min)' in navegacion_df.columns and not navegacion_df['Tiempo_Sesion(min)'].isnull().all():
        ts_q1 = navegacion_df['Tiempo_Sesion(min)'].quantile(0.25)
        ts_q3 = navegacion_df['Tiempo_Sesion(min)'].quantile(0.75)
        ts_iqr = ts_q3 - ts_q1
        if ts_iqr > 0:
            ts_lim_inf = ts_q1 - 1.5 * ts_iqr
            ts_lim_sup = ts_q3 + 1.5 * ts_iqr
            navegacion_df['Tiempo_Sesion(min)'] = np.where(navegacion_df['Tiempo_Sesion(min)'] < ts_lim_inf, ts_lim_inf, navegacion_df['Tiempo_Sesion(min)'])
            navegacion_df['Tiempo_Sesion(min)'] = np.where(navegacion_df['Tiempo_Sesion(min)'] > ts_lim_sup, ts_lim_sup, navegacion_df['Tiempo_Sesion(min)'])
    navegacion_df['Tiempo_Sesion(min)'].fillna(navegacion_df['Tiempo_Sesion(min)'].median(), inplace=True)
    navegacion_df['Clicks'].fillna(navegacion_df['Clicks'].median(), inplace=True)
    navegacion_df['Visitó_Carrito'].fillna(False, inplace=True)
    navegacion_df.loc[navegacion_df['Visitó_Carrito'] == False, 'Completó_Compra'] = False
    navegacion_df['Completó_Compra'].fillna(False, inplace=True) # Si Visitó_Carrito era NaN y se puso False

    print("Limpieza de datos completada.")
    return clientes_df, compras_df, navegacion_df

# --- PASO 3: TRANSFORMACIONES Y ANÁLISIS ---
def transformar_y_analizar(clientes_df, compras_df, navegacion_df):
    print("\n--- 3. Transformaciones y análisis ---")

    # Total_Compra
    compras_df['Total_Compra'] = compras_df['Precio'] * compras_df['Cantidad']
    print("\nDataFrame de Compras con 'Total_Compra' (primeras filas):")
    print(compras_df[['Compra_ID', 'Cliente_ID', 'Precio', 'Cantidad', 'Total_Compra']].head())

    # Dias_Desde_Registro
    clientes_df['Fecha_Registro'] = pd.to_datetime(clientes_df['Fecha_Registro'])
    clientes_df['Dias_Desde_Registro'] = (HOY - clientes_df['Fecha_Registro']).dt.days
    print("\nDataFrame de Clientes con 'Dias_Desde_Registro' (primeras filas):")
    print(clientes_df[['Cliente_ID', 'Nombre', 'Fecha_Registro', 'Dias_Desde_Registro']].head())

    # Métricas por cliente
    # Gasto total por cliente para calcular promedio
    gasto_total_por_cliente = compras_df.groupby('Cliente_ID')['Total_Compra'].sum().reset_index(name='Gasto_Total_Cliente')
    clientes_df = clientes_df.merge(gasto_total_por_cliente, on='Cliente_ID', how='left')
    clientes_df['Gasto_Total_Cliente'].fillna(0, inplace=True)
    promedio_gasto_cliente_general = clientes_df['Gasto_Total_Cliente'].mean()
    print(f"\nPromedio de gasto por cliente (general): ${promedio_gasto_cliente_general:.2f}")

    # Tiempo promedio de sesión por cliente
    navegacion_df['Fecha'] = pd.to_datetime(navegacion_df['Fecha'])
    tiempo_sesion_cliente = navegacion_df.groupby('Cliente_ID')['Tiempo_Sesion(min)'].mean().reset_index(name='Tiempo_Promedio_Sesion_Cliente')
    clientes_df = clientes_df.merge(tiempo_sesion_cliente, on='Cliente_ID', how='left')
    clientes_df['Tiempo_Promedio_Sesion_Cliente'].fillna(0, inplace=True)
    promedio_tiempo_sesion_general = clientes_df['Tiempo_Promedio_Sesion_Cliente'].mean() # Promedio de los promedios
    print(f"Tiempo promedio de sesión por cliente (promedio de promedios): {promedio_tiempo_sesion_general:.2f} min")

    # Porcentaje de sesiones que terminan en compra (general y por cliente)
    sesiones_por_cliente_agg = navegacion_df.groupby('Cliente_ID').agg(
        Total_Sesiones_Cliente=('Cliente_ID', 'count'),
        Sesiones_Con_Compra_Cliente=('Completó_Compra', 'sum')
    ).reset_index()
    sesiones_por_cliente_agg['Porcentaje_Conversion_Cliente'] = \
        (sesiones_por_cliente_agg['Sesiones_Con_Compra_Cliente'] / sesiones_por_cliente_agg['Total_Sesiones_Cliente']) * 100
    sesiones_por_cliente_agg['Porcentaje_Conversion_Cliente'].fillna(0, inplace=True)

    clientes_df = clientes_df.merge(sesiones_por_cliente_agg[['Cliente_ID', 'Total_Sesiones_Cliente', 'Porcentaje_Conversion_Cliente']], on='Cliente_ID', how='left')
    clientes_df['Total_Sesiones_Cliente'].fillna(0, inplace=True)
    clientes_df['Porcentaje_Conversion_Cliente'].fillna(0, inplace=True)

    porcentaje_sesiones_compra_global = (navegacion_df['Completó_Compra'].sum() / len(navegacion_df) * 100) if len(navegacion_df) > 0 else 0
    print(f"Porcentaje GLOBAL de sesiones que terminan en compra: {porcentaje_sesiones_compra_global:.2f}%")
    promedio_conversion_cliente = clientes_df['Porcentaje_Conversion_Cliente'].mean()
    print(f"Promedio del porcentaje de conversión POR CLIENTE: {promedio_conversion_cliente:.2f}%")
    
    return clientes_df, compras_df, navegacion_df

# --- PASO 4: ANÁLISIS DE RETENCIÓN ---
def analizar_retencion(clientes_df, compras_df):
    print("\n--- 4. Análisis de retención ---")
    if compras_df.empty or 'Fecha_Compra' not in compras_df.columns:
        print("No hay datos de compras para analizar la retención. Asignando 'Potencial (Sin Compra)' a todos.")
        clientes_df['Estado_Cliente'] = 'Potencial (Sin Compra)'
        clientes_df['Fecha_Ultima_Compra'] = pd.NaT
        clientes_df['Dias_Desde_Ultima_Compra'] = np.nan
    else:
        compras_df['Fecha_Compra'] = pd.to_datetime(compras_df['Fecha_Compra'])
        ultima_compra_cliente = compras_df.groupby('Cliente_ID')['Fecha_Compra'].max().reset_index()
        ultima_compra_cliente.rename(columns={'Fecha_Compra': 'Fecha_Ultima_Compra'}, inplace=True)
        clientes_df = clientes_df.merge(ultima_compra_cliente, on='Cliente_ID', how='left')
        clientes_df['Dias_Desde_Ultima_Compra'] = (HOY - clientes_df['Fecha_Ultima_Compra']).dt.days

    def clasificar_cliente_retencion(dias):
        if pd.isna(dias):
            return 'Potencial (Sin Compra)'
        if dias <= 15:
            return 'Nuevo'
        elif dias <= 30: # Entre 16 y 30 días
            return 'Activo'
        # El orden es importante: verificar >500 antes que >90
        elif dias > 500:
            return 'Muy Inactivo'
        elif dias > 90: # Entre 91 y 500 días
            return 'Inactivo'
        else: # Entre 31 y 90 días
            return 'En Riesgo'

    clientes_df['Estado_Cliente'] = clientes_df['Dias_Desde_Ultima_Compra'].apply(clasificar_cliente_retencion)
    
    conteo_estado_cliente = clientes_df['Estado_Cliente'].value_counts()
    porcentaje_estado_cliente = (clientes_df['Estado_Cliente'].value_counts(normalize=True) * 100).round(2)

    print("\nConteo de Clientes por Categoría de Retención:")
    print(conteo_estado_cliente)
    print("\nPorcentaje de Clientes por Categoría de Retención:")
    print(porcentaje_estado_cliente.astype(str) + '%')
    return clientes_df

# --- PASO 5: AGRUPACIÓN Y FILTRADO ---
def agrupar_y_filtrar(clientes_df, compras_df):
    print("\n--- 5. Agrupación y filtrado ---")
    if compras_df.empty:
        print("No hay datos de compras para realizar agrupaciones y filtrados.")
        return

    compras_con_ciudad_df = compras_df.merge(clientes_df[['Cliente_ID', 'Ciudad']], on='Cliente_ID', how='left')
    compras_con_ciudad_df['Ciudad'].fillna('Desconocida', inplace=True) # Asegurar que no haya NaNs

    facturacion_por_ciudad = compras_con_ciudad_df.groupby('Ciudad')['Total_Compra'].sum().sort_values(ascending=False)
    print("\nTop 5 ciudades con mayor facturación total:")
    print(facturacion_por_ciudad.head(5))

    analisis_por_categoria = compras_df.groupby('Categoría').agg(
        Monto_Total_Categoria=('Total_Compra', 'sum'),
        Ticket_Promedio_Categoria=('Total_Compra', 'mean')
    ).sort_values(by='Monto_Total_Categoria', ascending=False)
    print("\nAnálisis de compras por categoría (Monto Total y Ticket Promedio):")
    print(analisis_por_categoria)

# --- PASO 6: EXPORTACIÓN ---
def exportar_resumen(clientes_df, nombre_archivo="resumen_clientes.xlsx"):
    print("\n--- 6. Exportación ---")
    # Seleccionar y renombrar columnas para el resumen
    columnas_resumen = {
        'Cliente_ID': 'ID Cliente',
        'Nombre': 'Nombre Cliente',
        'Estado_Cliente': 'Estado',
        'Gasto_Total_Cliente': 'Total Gastado ($)',
        'Total_Sesiones_Cliente': 'Número de Sesiones',
        'Tiempo_Promedio_Sesion_Cliente': 'Tiempo Promedio Sesión (min)',
        'Ciudad': 'Ciudad',
        'Fecha_Registro': 'Fecha Registro',
        'Dias_Desde_Registro': 'Días Desde Registro',
        'Fecha_Ultima_Compra': 'Fecha Última Compra',
        'Dias_Desde_Ultima_Compra': 'Días Desde Última Compra',
        'Porcentaje_Conversion_Cliente': '% Conversión Sesiones'
    }
    # Asegurarse que todas las columnas existan, crear con NaT/NaN/0 si no.
    for col_original in columnas_resumen.keys():
        if col_original not in clientes_df.columns:
            if 'Fecha' in col_original:
                 clientes_df[col_original] = pd.NaT
            elif 'Total Gastado' in columnas_resumen[col_original] or 'Número de Sesiones' in columnas_resumen[col_original] or 'Tiempo Promedio' in columnas_resumen[col_original] or '%' in columnas_resumen[col_original]:
                 clientes_df[col_original] = 0
            else:
                 clientes_df[col_original] = np.nan


    resumen_clientes_exportar_df = clientes_df[list(columnas_resumen.keys())].copy()
    resumen_clientes_exportar_df.rename(columns=columnas_resumen, inplace=True)
    
    ruta_completa_excel = os.path.join(CARPETA_SALIDA, nombre_archivo)
    try:
        resumen_clientes_exportar_df.to_excel(ruta_completa_excel, index=False, sheet_name='ResumenClientes')
        print(f"\nResumen de clientes exportado exitosamente a '{ruta_completa_excel}'")
    except Exception as e:
        print(f"\nError al exportar a Excel: {e}")

# --- EJECUCIÓN PRINCIPAL DEL SCRIPT ---
if __name__ == "__main__":
    print("Iniciando el análisis de datos de Smartech...")

    # 0. Generar Datos
    clientes_df_orig = generar_datos_clientes(num_filas=40) # Aumentamos para más variedad
    compras_df_orig = generar_datos_compras(clientes_df_orig, num_filas=60)
    navegacion_df_orig = generar_datos_navegacion(clientes_df_orig, num_filas=70)
    
    # Crear copias para no modificar los originales durante la limpieza
    clientes_df = clientes_df_orig.copy()
    compras_df = compras_df_orig.copy()
    navegacion_df = navegacion_df_orig.copy()

    # 1. Carga (simulada) y Revisión de los Datos
    print("\n--- 1. Carga y revisión de los datos (antes de la limpieza) ---")
    resumen_problemas = {}
    resumen_problemas["clientes"] = revisar_dataframe(clientes_df, "Clientes (Original)")
    resumen_problemas["compras"] = revisar_dataframe(compras_df, "Compras (Original)")
    resumen_problemas["navegacion"] = revisar_dataframe(navegacion_df, "Navegación (Original)")

    print("\n--- Resumen de problemas detectados por archivo (antes de limpieza) ---")
    for nombre_df, problemas in resumen_problemas.items():
        print(f"{nombre_df.capitalize()}: {problemas['nulos']} nulos, "
              f"{problemas['faltantes']} faltantes, "
              f"{problemas['duplicados']} duplicados, "
              f"{problemas['outliers_estimados']} outliers (estimados).")

    # 2. Limpieza de Datos
    clientes_df, compras_df, navegacion_df = limpiar_datos(clientes_df, compras_df, navegacion_df)
    
    print("\n--- Revisión de los datos DESPUÉS de la limpieza ---")
    revisar_dataframe(clientes_df, "Clientes (Limpio)")
    revisar_dataframe(compras_df, "Compras (Limpio)")
    revisar_dataframe(navegacion_df, "Navegación (Limpio)")


    # 3. Transformaciones y Análisis
    clientes_df, compras_df, navegacion_df = transformar_y_analizar(clientes_df, compras_df, navegacion_df)

    # 4. Análisis de Retención
    clientes_df = analizar_retencion(clientes_df, compras_df)

    # 5. Agrupación y Filtrado
    agrupar_y_filtrar(clientes_df, compras_df)

    # 6. Exportación
    exportar_resumen(clientes_df, nombre_archivo="resumen_clientes_smartech.xlsx")

    print("\n--- Proceso de Análisis Completado ---")