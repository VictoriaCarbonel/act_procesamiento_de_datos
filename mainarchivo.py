# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 16:04:30 2025

@author: micag
"""

# -*- coding: utf-8 -*-
"""
Created on Friday Feb 14 18:30:27 2025
Alumnos: Gonzalo Caporaletti, Victoria Carbonel, Micaela Gonzalez Dardik
Tema: Trabajo Práctico 01. Manejo y visualización de Datos
Fecha de entrega: 23 de febrero de 2025 
"""

#%% PROCESAMIENTO Y LIMPIEZA DE DATOS

#%%% IMPORTS Y CARGA DE DATOS
import pandas as pd
import numpy as np
import duckdb as dd
import re
import unicodedata
import matplotlib.pyplot as plt
import seaborn as sns
# Las bases de datos necesarias para el Trabajo las guardamos en la carpeta tp01

#%% FUNCIONES
def reemplazar_si_empieza(texto):                           #cambia los nombres de comuna a ciudad autonoma de buenos aires
    if isinstance(texto, str):                              # Verifica que es un string
        if texto.startswith("comuna"):
            return "ciudad autonoma de buenos aires"        # Nuevo valor
    return texto                                            # Deja el valor original si no coincide



def quitar_tildes(texto):                                   # Saca las tildes y convierte a minúsculas
    if isinstance(texto, str):                              # Verifica que sea texto
        return ''.join(
            c for c in unicodedata.normalize('NFD', texto)
            if unicodedata.category(c) != 'Mn'
        ).lower()                                           # Convierte a minúsculas 
    return texto

# %% Cargamos las Bases de Datos de cada fuente

CC = pd.read_csv("centros_culturales.csv")

ee = pd.read_excel("2022_padron_oficial_establecimientos_educativos.xlsx", skiprows=6)

pp = pd.read_excel("padron_poblacion.xlsx", skiprows=12)


#%% LIMPIEZA DE DATOS
#%% BASE de DATOS: CC

CC = CC.replace({0: 's/d', '-': 's/d'})             # Reemplaza 0 y '-' en todas las columnas
CC = CC.fillna('s/d')                               # Reemplaza los valores nulos (NaN)
CC = CC.reset_index()                               # convierto el índice en una columna para identificar los CC
CC.rename(columns={'index': 'ID_CC'}, inplace=True) # Lo renombro
CC.rename(columns={'Mail ': 'Mail'}, inplace=True)  # Elimino el espacio en el nombre de la columna Mail

'---------------------------------corrijo celdas desplazadas--------------------------------------------'

conn = dd.connect(database='CC.duckdb', read_only=False) # Creo un archivo de la base de datos par usarlo como base en duckdb
conn.execute("DROP TABLE IF EXISTS CC")                  # Eliminar la tabla CC si existe (para evitar conflictos si se ejecuta más de una vez)
conn.execute("CREATE TABLE CC AS SELECT * FROM CC")      # Creo la tabla base CC a partir del DataFrame en el archivo

consultaSQL = """
                    UPDATE CC
                    SET 
                    Domicilio = 'Falucho 1418',
                    Piso = '',              
                    CP = '',  
                    cod_area = '11',
                    Telefóno = '44400547',
                    Mail = 'Ignaciodevedia@gmail.com',
                    Web = '',               
                    InfoAdicional = '',
                    Latitud = '-34.38956050',
                    Longitud = '-58.73757020',
                    TipoLatitudLongitud = 'Precisa',
                    Fuente = 'Puntos de Cultura 2020',           
                    año_inicio = '',
                    Capacidad = '',      
                    Actualizacion = '2020'     
                    WHERE Nombre = 'Casa Popular Marielle Franco';
"""
conn.execute(consultaSQL)

consultaSQL = """
                    UPDATE CC
                    SET 
                    Latitud = '-45.81641540',
                    Longitud = '-67.45550320',
                    TipoLatitudLongitud = 'Precisa',
                    Fuente = 'Puntos de Cultura 2020',           
                    Capacidad = 0 ,      
                    Actualizacion = '2020'     
                    WHERE Nombre = 'Espacio Cultural "Kultural 5"';
"""
conn.execute(consultaSQL)

CC = conn.execute("SELECT * FROM CC").df()

#Elimino las columnas que no usaremos y Paso Departamento a Minúscula
consultaSQL = '''SELECT ID_CC, 
                 ID_PROV, ID_DEPTO, 
                 Provincia, 
                 lower(Departamento) AS Departamento, 
                 Nombre,
                 Mail, 
                 Capacidad 
                 FROM CC'''
       
CC = dd.sql(consultaSQL).df()

CC["Departamento"] = CC["Departamento"].apply(quitar_tildes)    #Elimino las tildes
CC['Departamento'] = CC['Departamento'].replace({               #Cambio los nombres de los departamentos que no coincidan con el df de Departamentos
    'grl. jose de san martin': 'general jose de san martin',
    "san nicolas de los arroyos" : "san nicolas",
    "o' higgins": "o'higgins",
    'primero de mayo': "1º de mayo",
})



#%%BASE de DATOS: ee

ee["Departamento"] = ee["Departamento"].apply(quitar_tildes)

ee = ee.rename(columns={"Jurisdicción": "Provincia"})

'''Arreglo a la base de datos ee'''

#1 cambio los departamentos que empiezan con comuna a ciudad autonoma de buenos aires
ee["Departamento"] = ee["Departamento"].apply(reemplazar_si_empieza) 

#2 reemplazo lugares vacios por NaN
ee = ee.replace(' ', np.nan)  # Convierte espacios en blanco en NaN
ee = ee.replace('', np.nan)  # Convierte strings vacíos en NaN

#3 reemplazo NaNs por 0
ee = ee.fillna(0)  # Rellena NaN con s/d

#modifico los 0 por s/d en las columnas que corresponde
columnas_a_modificar = ['Provincia', 'Cueanexo', 'Nombre', 'Sector', 'Ámbito', 'Domicilio', 'C. P.', 'Código de área', 'Teléfono', 'Código de localidad', 'Localidad', 'Departamento', 'Mail']
ee[columnas_a_modificar] = ee[columnas_a_modificar].replace({0: 's/d', '-': 's/d'})

#4 Filtro las filas donde la columna 'Común' tenga un valor de 1
ee = ee[ee["Común"].astype(int) == 1] # 1 era un string, forzosamente lo transformo en un int

#5 Cambio específico de algunos departamentos que no coinciden con la entidad departamento
ee['Departamento'] = ee['Departamento'].replace({
    '1§ de mayo': '1º de mayo',
    'coronel de marina l rosales': 'coronel de marina leonardo rosales',
    'coronel felipe varela': 'general felipe varela',
    'o higgins': "o'higgins",
    'doctor manuel belgrano': "dr. manuel belgrano",
    'mayor luis j fontana': "mayor luis j. fontana",
    'general juan f quiroga' : 'general juan facundo quiroga',
    'general ocampo' : 'general ortiz de ocampo',
    'juan f ibarra' : 'juan felipe ibarra',
})

#6 Cambio específico de algunas provincias para que coincidan con la entidad provincia
ee['Provincia'] = ee['Provincia'].replace({
    'Ciudad de Buenos Aires': 'Ciudad Autónoma de Buenos Aires',
    'Tierra del Fuego': 'Tierra del Fuego, Antártida e Islas del Atlántico Sur',
})

#7 Selecciono solo las columnas que necesito para trabajar y analizar
columnas_necesarias = [
    "Provincia", "Cueanexo", "Nombre", "Departamento",
    "Nivel inicial - Jardín maternal", "Nivel inicial - Jardín de infantes",
    "Primario", "Secundario", "Secundario - INET", "SNU", "SNU - INET"
]
ee = ee[columnas_necesarias]

# %% BASE de DATOS: Población
# renombro las columnas para que tenga coherencia con los datos que representa
pp = pp.rename(columns={"Unnamed: 1": "Edad", "Unnamed: 2": "Poblacion", "Unnamed: 3": "%", "Unnamed: 4": "Acumulado %"})

# elimino aquellas filas que están completamente vacías
pp = pp.dropna(how="all")

# elimino las filas donde en edad dice 'Total'
pp = pp[pp['Edad'] != 'Total']

# nuevas columnas para almacenar area y comuna
pp["Área"], pp["Comuna"], area_actual, comuna_actual = None, None, None, None

# itero las filas para detectar areas y comunas
for i, row in pp.iterrows():
    if isinstance(row["Edad"], str) and "AREA #" in row["Edad"]:
        area_actual, comuna_actual, pp.at[i, "Edad"] = row["Edad"], row["Poblacion"], None
    pp.at[i, "Área"], pp.at[i, "Comuna"] = area_actual, comuna_actual


# elimino filas que quedaron con NaN en %
pp = pp.dropna(subset=["%"])
# elimino las filas "encabezado" donde en Edad, no hay una edad sino un título
pp = pp[pp['Edad'] != 'Edad']

# elimimno "Unnamed:0" que es una columna que no aporta información
pp = pp.drop(columns=['Unnamed: 0'])
# verifico que no haya ningun nan en mi pp (chequeo la suma de cuántos nan hay)
print(pp.isna().sum().sum())



"""Ahora vamos a juntar la data de las 3 fuentes de datos.
		Para eso vimos que en las demas fuentes (especificamente en la de centros culturales)
		no hay información de las comunas. Solamente filtra por departamento.
		Por lo tanto tomamos la decisión de que todas las comunas pasen a ser parte
		del "Departamento" de la C.A.B.A."""  

# definir una función para reemplazar valores según la palabra inicial
pp["Comuna"] = pp["Comuna"].where(~pp["Comuna"].astype(str).str.startswith("Comuna"), "ciudad autonoma de buenos aires")
pp = pp.rename(columns={"Comuna": "Departamento", "Poblacion": "Poblacion"})
# para este informe vamos a necesitar solamente la poblacion por grupo etario por depto
# por lo tanto eliminamos los porcentajes, el acumulado, y el ara
pp = pp.drop(columns=['%', 'Acumulado %', 'Área'])
# reordeno las columnas
pp = pp[["Departamento", "Edad", "Poblacion"]]

del area_actual, comuna_actual, i, row


#%% CREACIÓN DE LA ENTIDAD Departamentos y Provincias
'------------------------------------Provincia---------------------------------------------------'

consultaSQL = ''' SELECT DISTINCT Provincia, ID_PROV FROM CC ORDER BY ID_PROV '''

Provincias = dd.sql(consultaSQL).df()

ee.drop(columns=['ID_PROV'], errors='ignore', inplace=True)

ee = ee.merge(Provincias, on='Provincia', how='left')

'------------------------------------Departamento------------------------------------------------'
consultaSQL = ''' SELECT DISTINCT Departamento, Provincia FROM ee ORDER BY Departamento '''

Departamentos = dd.sql(consultaSQL).df()

#completocon los departamentos faltantes
nuevos_departamentos = pd.DataFrame({
    'Departamento': [
        'pigue', 
        'veronica', 
        'coronel brandsen', 
        'santa fe', 
        'san miguel del monte',
        'tolhuin'
    ]
})

Departamentos = pd.concat([Departamentos, nuevos_departamentos], ignore_index=True)
#agreggo el ID_DEPTO
Departamentos['ID_DEPTO'] = range(1, len(Departamentos) + 1)

consultaSQL = '''SELECT DISTINCT ID_DEPTO, Departamento, Provincia FROM Departamentos ORDER BY ID_DEPTO'''
Departamentos = dd.sql(consultaSQL).df()

faltantes = {
    "pigue": "Buenos Aires",
    "veronica": "Buenos Aires",
    "coronel brandsen": "Buenos Aires",
    "santa fe": "Santa Fe",
    "san miguel del monte": "Buenos Aires",
    "tolhuin": "Tierra del Fuego, Antártida e Islas del Atlántico Sur"
}

# Actualizo la columna Provincia para que corresponda al Departamento
Departamentos["Provincia"] = Departamentos.apply(lambda row: faltantes.get(row["Departamento"], row["Provincia"]), axis=1)

#cambio Provincia por ID_PROV
Departamentos = Departamentos.merge(Provincias, on= "Provincia", how='left')
Departamentos.drop(columns=['Provincia'], inplace=True)
#%%

CC.drop(columns=['ID_DEPTO'], inplace=True)                                    #Elimino la antigua columna de ID_DEPTO
CC = CC.merge(Departamentos, on=['Departamento', 'ID_PROV'], how='left')       #Asigno el ID_DEPTO correcto a cada fila
# %%
ee = ee.merge(Departamentos, on=['Departamento', 'ID_PROV'], how='left')       # Agrego código de departamento

# %%
pp['Departamento'] = pp['Departamento'].apply(quitar_tildes)
pp = pp.merge(Departamentos, on='Departamento', how='left')
pp.drop(columns=['Departamento'], inplace=True)
pp.drop(columns=['ID_PROV'], inplace=True)
# junto todos los de CABA
pp = pp.groupby(['Edad', 'ID_DEPTO'], as_index=False).agg(
    {'Poblacion': 'sum'}
)

#%%
# Armamos las bases  a partir del DER que planteamos
# mediante funciones de Pandas y consultas SQL

consultaSQL= """ 
                SELECT DISTINCT Cueanexo,
                Nombre, 
                ID_DEPTO
                FROM ee;
                            """
Establecimientos_Educativos = dd.sql(consultaSQL).df()

consultaSQL = ''' SELECT DISTINCT ID_CC,
                          CC.Nombre, 
                          CC.Capacidad,
                          CC.ID_DEPTO,
                          CC.ID_PROV
                  FROM CC
                  ORDER BY Nombre; '''
                  
Centros_Culturales = dd.sql(consultaSQL).df()

           
consultaSQL = """
                  SELECT 1 AS id_nivelEducativo, 'Nivel inicial - Jardín Maternal' AS Nombre UNION ALL
                  SELECT 2, 'Nivel inicial - Jardín de Infantes' UNION ALL
                  SELECT 3, 'Primario' UNION ALL
                  SELECT 4, 'Secundario' UNION ALL
                  SELECT 5, 'Secundario - INET' UNION ALL
                  SELECT 6, 'SNU' UNION ALL
                  SELECT 7, 'SNU - INET';
           """
Nivel_Educativo = dd.sql(consultaSQL).df()

consultaSQL = """
                    SELECT DISTINCT cueanexo, id_nivelEducativo
                    FROM ee AS e
                    INNER JOIN Nivel_Educativo AS m 
                    ON (e."Nivel inicial - Jardín Maternal" = '1' AND m.Nombre = 'Nivel inicial - Jardín Maternal') OR
                    (e."Nivel inicial - Jardín de Infantes" = '1' AND m.Nombre = 'Nivel inicial - Jardín de Infantes') OR
                    (e."Primario" = '1' AND m.Nombre = 'Primario') OR
                    (e."Secundario" = '1' AND m.Nombre = 'Secundario') OR
                   (e."Secundario - INET" = '1' AND m.Nombre = 'Secundario - INET') OR
                   (e."SNU" = '1' AND m.Nombre = 'SNU') OR
                   (e."SNU - INET" = '1' AND m.Nombre = 'SNU - INET')
                   ORDER BY cueanexo;
              """
Nivel_Educativo = dd.sql(consultaSQL).df()            

consultaSQL= '''SELECT * FROM pp'''

Poblacion = dd.sql(consultaSQL).df()

'-----------------------------------------Creo la entidad débil Mails_CC-----------------------------'
Mails = CC.copy()
Mails["Mail"] = Mails["Mail"].fillna("").astype(str)                                                      # Me aseguro que la columna de mails es string y saco nans
email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"                                         # Formato generico"" para extraer mails completos
Mails["mails_extraidos"] = Mails["Mail"].apply(lambda x: re.findall(email_pattern, x))                    # Extraigo correos, los pongo en una columna en forma de lista
Mails = Mails.explode("mails_extraidos", ignore_index=True)                                               # Exploto las listas en filas separadas
Mails = Mails.drop(columns=["Mail"]).rename(columns={"mails_extraidos": "Mail"})                           # Renombro la columna
Mails = Mails.drop(columns=[ 'ID_PROV', 'ID_DEPTO', 'Provincia', 'Departamento', 'Nombre', 'Capacidad' ]) # Elimino los atributos que no forman parte de la entidad


#%% Exporto los DF

Centros_Culturales.to_csv('Centro_Cultural.csv')
Establecimientos_Educativos.to_csv('Establecimientos_Educativos.csv')
Nivel_Educativo.to_csv('Nivel_Educativo.csv')
Nivel_Educativo.to_csv('Nivel_Educativo.csv')
Departamentos.to_csv('Departamentos.csv')
Provincias.to_csv('Provincias.csv')
Mails.to_csv('Mails.csv')
Poblacion.to_csv('Poblacion.csv')


        #%% EJERCICIOS DE CONSULTAS SQL
#%%% EJERCICIO 1

# para obtener escuelas y habitantes por edad
consultaSQL_educacion = """
              SELECT p.Provincia AS Provincia,
                     d.Departamento AS Departamento,
                     COUNT(CASE WHEN n.Nombre = 'Primario' THEN 1 END) AS Cantidad_Primarias,
                     COUNT(CASE WHEN n.Nombre = 'Secundario' THEN 1 END) AS Cantidad_Secundarias,
                     COUNT(CASE WHEN n.Nombre = 'Nivel inicial - Jardín Maternal' THEN 1 END) AS Cantidad_Jardin_Maternal,
                     COUNT(CASE WHEN n.Nombre = 'Nivel inicial - Jardín de Infantes' THEN 1 END) AS Cantidad_Jardin_Infantes,
                     SUM(CASE WHEN e.Edad BETWEEN 0 AND 4 THEN e.Cantidad ELSE 0 END) AS Habitantes_0_4,
                     SUM(CASE WHEN e.Edad BETWEEN 5 AND 9 THEN e.Cantidad ELSE 0 END) AS Habitantes_5_9,
                     SUM(CASE WHEN e.Edad BETWEEN 10 AND 14 THEN e.Cantidad ELSE 0 END) AS Habitantes_10_14,
                     SUM(CASE WHEN e.Edad BETWEEN 15 AND 19 THEN e.Cantidad ELSE 0 END) AS Habitantes_15_19
              FROM Departamentos AS d
              JOIN Provincias AS p ON d.ID_PROV = p.ID_PROV
              LEFT JOIN nivelEducativo_ee AS ne ON d.ID_DEPTO = ne.ID_DEPTO
              LEFT JOIN nivelEducativo AS n ON ne.id_nivelEducativo = n.id_nivelEducativo
              LEFT JOIN HabitantesPorEdad AS e ON d.ID_DEPTO = e.ID_DEPTO
              WHERE n.Modalidad = 'Común'
              GROUP BY p.Provincia, d.Departamento
              ORDER BY p.Provincia ASC, Cantidad_Primarias DESC
              """
resultado_educacion = conn.execute(consultaSQL_educacion).df()
print("reporte de departamentos con escuelas y habitantes por edad:")
print(resultado_educacion)


#%%% EJERCICIO 2

# obtener departamentos con sus provincias
consultaSQL_departamentos = """
                SELECT d.ID_DEPTO, d.Departamento, d.ID_PROV, p.Provincia
                FROM Departamentos AS d
                JOIN Provincias AS p ON d.ID_PROV = p.ID_PROV
              """
departamentos_con_provincias = conn.execute(consultaSQL_departamentos).df()
print("departamentos con sus provincias:")
print(departamentos_con_provincias)

# %% 

# centros culturales con capacidad > 100
consultaSQL_cc_filtrados = """
                SELECT cc.ID_CC, cc.Nombre, cc.ID_DEPTO, cc.ID_PROV, cc.Capacidad
                FROM Centros_Culturales AS cc
                WHERE cc.Capacidad NOT IN ('s/d')
                AND CAST(cc.Capacidad AS INTEGER) > 100
              """
centros_culturales_filtrados = conn.execute(consultaSQL_cc_filtrados).df()
print("centros culturales con capacidad > 100:")
print(centros_culturales_filtrados)

# %% 

# uno los datos de centros culturales con departamentos y provincias
consultaSQL_final = """
                SELECT p.Provincia AS Provincia,
                       d.Departamento AS Departamento,
                       COALESCE(cc.Cantidad_CC, 0) AS Cantidad_CC
                FROM Departamentos AS d
                JOIN Provincias AS p ON d.ID_PROV = p.ID_PROV
                LEFT JOIN (
                    SELECT cc.ID_DEPTO, cc.ID_PROV, COUNT(cc.ID_CC) AS Cantidad_CC
                    FROM Centros_Culturales AS cc
                    WHERE cc.Capacidad NOT IN ('s/d')
                    AND CAST(cc.Capacidad AS INTEGER) > 100
                    GROUP BY cc.ID_DEPTO, cc.ID_PROV
                ) AS cc ON d.ID_DEPTO = cc.ID_DEPTO AND d.ID_PROV = cc.ID_PROV
                ORDER BY p.Provincia ASC, Cantidad_CC DESC
              """
reporte_final = conn.execute(consultaSQL_final).df()
print("reporte final de departamentos con cantidad de cc (capacidad > 100):")
print(reporte_final)

#%%% EJERCICIO 3

# cant de CC, EE y poblacion total por depto
consultaSQL_reporte = """
              SELECT p.Provincia AS Provincia,
                     d.Departamento AS Departamento,
                     COALESCE(cc.Cantidad_CC, 0) AS Cantidad_CC,
                     COALESCE(ee.Cantidad_EE, 0) AS Cantidad_EE,
                     SUM(h.Cantidad) AS Poblacion_Total
              FROM Departamentos AS d
              JOIN Provincias AS p ON d.ID_PROV = p.ID_PROV
              LEFT JOIN (
                  SELECT ID_DEPTO, COUNT(ID_CC) AS Cantidad_CC
                  FROM Centros_Culturales
                  GROUP BY ID_DEPTO
              ) AS cc ON d.ID_DEPTO = cc.ID_DEPTO
              LEFT JOIN (
                  SELECT d.ID_DEPTO, COUNT(ne.ID_EE) AS Cantidad_EE
                  FROM nivelEducativo_ee AS ne
                  JOIN Departamentos AS d ON ne.ID_DEPTO = d.ID_DEPTO
                  JOIN nivelEducativo AS n ON ne.id_nivelEducativo = n.id_nivelEducativo
                  WHERE n.Modalidad = 'Común'
                  GROUP BY d.ID_DEPTO
              ) AS ee ON d.ID_DEPTO = ee.ID_DEPTO
              LEFT JOIN HabitantesPorEdad AS h ON d.ID_DEPTO = h.ID_DEPTO
              GROUP BY p.Provincia, d.Departamento
              ORDER BY ee.Cantidad_EE DESC, cc.Cantidad_CC DESC, p.Provincia ASC, d.Departamento ASC
              """
resultado_reporte = conn.execute(consultaSQL_reporte).df()
print("reporte de departamentos con cantidad de CC, EE y población total:")
print(resultado_reporte)

#%%% EJERCICIO 4
consulta_provincias_departamentos = """
                           SELECT p.Provincia, d.Departamento
                           FROM Provincias AS p
                           INNER JOIN Departamentos AS d 
                           ON p.ID_PROV = d.ID_PROV
                           """
provincias_departamentos = dd.sql(consulta_provincias_departamentos).df()

# contar los dominios de mail usados en los CC
consulta_dominios = """
           SELECT d.Departamento, p.Provincia,
                  SPLIT_PART(mail, '@', 2) AS Dominio,
                  COUNT(*) AS Cantidad
           FROM Centros_Culturales AS cc
           JOIN Provincias AS p ON cc.ID_PROV = p.ID_PROV
           JOIN Departamentos AS d ON cc.ID_DEPTO = d.ID_DEPTO
           GROUP BY d.Departamento, p.Provincia, Dominio
           ORDER BY d.Departamento, Cantidad DESC
           """
dominios_cc = dd.sql(consulta_dominios).df()

print("dominios de mail usados por centros culturales por departamento:")
print(dominios_cc)

#%% EJERCICIOS DE VISUALIZACIÓN DE DATOS
#%%% EJERCICIO 1



#cant de CC por provincia
consulta_cc_por_provincia = """
              SELECT p.Provincia, COUNT(cc.ID_CC) AS Cantidad_CC
              FROM Provincias AS p
              LEFT JOIN Centros_Culturales AS cc ON p.ID_PROV = cc.ID_PROV
              GROUP BY p.Provincia
              ORDER BY Cantidad_CC DESC
              """
cc_por_provincia = dd.sql(consulta_cc_por_provincia).df()

plt.figure(figsize=(10, 6))
sns.barplot(data=cc_por_provincia, x='Cantidad_CC', y='Provincia', palette='viridis')
plt.title('Cantidad de CC por Provincia')
plt.xlabel('Cantidad de CC')
plt.ylabel('Provincia')
plt.show()

#%%% EJERCICIO 2

# consulta 1 (de educación)
resultado_educacion = conn.execute(consultaSQL_educacion).df()

# creo un df para los grupos etarios y la cantidad de primarias
grafico_data = resultado_educacion.melt(id_vars=['Provincia', 'Departamento'], 
                                         value_vars=['Habitantes_0_4', 'Habitantes_5_9', 
                                                     'Habitantes_10_14', 'Habitantes_15_19'],
                                         var_name='Grupo_Etario', 
                                         value_name='Habitantes')


plt.figure(figsize=(12, 8))
sns.barplot(data=grafico_data, x='Habitantes', y='Departamento', hue='Grupo_Etario', palette='Set2')
plt.title('Habitantes por Grupo Etario en Departamentos')
plt.xlabel('Cantidad de Habitantes')
plt.ylabel('Departamento')
plt.legend(title='Grupo Etario')
plt.show()


#%%% EJERCICIO 3
'''
Realizar un boxplot por cada provincia, de la cantidad de EE por cada
departamento de la provincia. Mostrar todos los boxplots en una misma
figura, ordenados por la mediana de cada provincia.
'''

consultaSQL = '''SELECT ID_DEPTO, 
                 COUNT(*) AS Cantidad_de_EE 
                 FROM Establecimientos_Educativos
                 GROUP BY ID_DEPTO 
                 ORDER BY Cantidad_de_EE''' 

Cantidad_de_EE = dd.sql(consultaSQL).df()                                                  #agrupo las escuelas por departamento para ver la cantidad en cada depto

Cantidad_de_EE = Cantidad_de_EE.merge(Departamentos, on='ID_DEPTO', how='left')            #agrego ID_PROV para poder agruparlos en provincias
Cantidad_de_EE.drop(columns=['Departamento'], inplace=True)                                #elimino el nombre del departamento
Cantidad_de_EE = Cantidad_de_EE.merge(Provincias, on='ID_PROV', how='left')                #agrego el nombre de las provincias

medianas = Cantidad_de_EE.groupby("Provincia")["Cantidad_de_EE"].median().sort_values()    #creo un df con las medianas de la cantidad de dapartamentos por provincia
orden_provincias = medianas.index.tolist()

# grafico
plt.figure(figsize=(12,8))
sns.boxplot(x="Provincia", y="Cantidad_de_EE", data=Cantidad_de_EE, order=orden_provincias)
plt.xticks(rotation=90)
plt.xlabel("Provincia")
plt.ylabel("Cantidad de EE por Departamento")
plt.title("Distribución de EE por Departamento en cada Provincia (ordenado por sus medianas)")
#plt.ylim(min(Cantidad_de_EE['Cantidad_de_EE'])-50,1150)
plt.grid(True)
plt.show()
