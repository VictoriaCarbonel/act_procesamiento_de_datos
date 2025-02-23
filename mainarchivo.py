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

# %%
pp['codigo_provincia'] = pp["Área"].str[7:9]  # Tomar los caracteres 1 y 2
pp['codigo_depto'] = pp["Área"].str[9:]  # Tomar los últimos 3 caracteres
pp = pp.drop(columns=["Área"])

# elimino las filas que no pertenecen a ningun departamento (son parte del resumen)

indice_resumen = pp[pp["Edad"] == "RESUMEN"].index.min()

# Si existe 'Resumen', elimino todas las filas desde esa posición en adelante
if not pd.isna(indice_resumen):
    pp = pp.loc[:indice_resumen-1]

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
# %%
pp_agrupado = (
    pp[pp["codigo_provincia"] == "02"]
    .groupby(["Edad"], as_index=False)
    .agg({"Poblacion": "sum", "%": "sum", "Acumulado %": "sum"})
)
# Asignar valores fijos
pp_agrupado["codigo_provincia"] = "02"
pp_agrupado["codigo_depto"] = "000"
pp_agrupado["Departamento"] = "ciudad autonoma de buenos aires"  # O podés dejarlo en blanco si no importa
# filtrar el df orihinal quitando 02
pp = pp[pp["codigo_provincia"] != "02"]
# Concatenar el DataFrame original con el agrupado
pp = pd.concat([pp_agrupado, pp], ignore_index=True)

# para este informe vamos a necesitar solamente la poblacion por grupo etario por depto
# por lo tanto eliminamos los porcentajes, el acumulado, y el ara
pp = pp.drop(columns=['%', 'Acumulado %', 'Área'])
# reordeno las columnas
pp = pp[["Departamento", "Edad", "Poblacion"]]

del area_actual, comuna_actual, i, row


#%% CREACIÓN DE LA ENTIDAD Departamentos y Provincias
'------------------------------------Provincia---------------------------------------------------'

consultaSQL = ''' SELECT DISTINCT ID_PROV, Provincia FROM CC ORDER BY ID_PROV '''

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
Establecimientos_E = dd.sql(consultaSQL).df()

consultaSQL = ''' SELECT DISTINCT ID_CC,
                          CC.Nombre, 
                          CC.Capacidad,
                          CC.ID_DEPTO
                  FROM CC
                  ORDER BY Nombre; '''
                  
Centros_C = dd.sql(consultaSQL).df()

           
consultaSQL = """
                  SELECT 1 AS id_Nivel_Educativo, 'Nivel inicial - Jardín Maternal' AS Nombre UNION ALL
                  SELECT 2, 'Nivel inicial - Jardín de Infantes' UNION ALL
                  SELECT 3, 'Primario' UNION ALL
                  SELECT 4, 'Secundario' UNION ALL
                  SELECT 5, 'Secundario - INET' UNION ALL
                  SELECT 6, 'SNU' UNION ALL
                  SELECT 7, 'SNU - INET';
           """
Nivel_Educativo = dd.sql(consultaSQL).df()

consultaSQL = """
                    SELECT DISTINCT cueanexo, id_Nivel_Educativo
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
Nivel_Educativo_de_ee = dd.sql(consultaSQL).df()            

consultaSQL= '''SELECT * FROM pp'''

Reporte_Demografico = dd.sql(consultaSQL).df()

'-----------------------------------------Creo la entidad débil Mails_CC-----------------------------'
Mails = CC.copy()
Mails["Mail"] = Mails["Mail"].fillna("").astype(str)                                                      # Me aseguro que la columna de mails es string y saco nans
email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"                                         # Formato generico"" para extraer mails completos
Mails["mails_extraidos"] = Mails["Mail"].apply(lambda x: re.findall(email_pattern, x))                    # Extraigo correos, los pongo en una columna en forma de lista
Mails = Mails.explode("mails_extraidos", ignore_index=True)                                               # Exploto las listas en filas separadas
Mails = Mails.drop(columns=["Mail"]).rename(columns={"mails_extraidos": "Mail"})                           # Renombro la columna
Mails = Mails.drop(columns=[ 'ID_PROV', 'ID_DEPTO', 'Provincia', 'Departamento', 'Nombre', 'Capacidad' ]) # Elimino los atributos que no forman parte de la entidad

Departamentos['Departamento'] = Departamentos['Departamento'].str.title()

#%% Exporto los DF

Centros_C.to_csv('Centros_Cs.csv', index=False)
Establecimientos_E.to_csv('Establecimientos_E.csv', index=False)
Nivel_Educativo.to_csv('Nivel_Educativo.csv', index=False)
Nivel_Educativo_de_ee.to_csv('Nivel_Educativo_de_ee.csv', index=False)
Departamentos.to_csv('Departamentos.csv', index=False)
Provincias.to_csv('Provincias.csv', index=False)
Mails.to_csv('Mails.csv', index=False)
Reporte_Demografico.to_csv('Reporte_Demografico.csv', index=False)


#%% EJERCICIOS DE CONSULTAS SQL
#%%% EJERCICIO 1

# Consulto Establecimientos Educativos por Nivel en cada departamento
consultaSQL = '''
            SELECT 
                d.ID_DEPTO,
                d.Departamento,
                p.Provincia,
                SUM(CASE WHEN ne.id_Nivel_Educativo = 1 THEN 1 ELSE 0 END) AS Cant_Escuelas_Inicial,
                SUM(CASE WHEN ne.id_Nivel_Educativo = 2 THEN 1 ELSE 0 END) AS Cant_Escuelas_Primaria,
                SUM(CASE WHEN ne.id_Nivel_Educativo = 3 THEN 1 ELSE 0 END) AS Cant_Escuelas_Secundaria
                FROM Departamentos AS d
                JOIN Provincias AS p ON d.ID_PROV = p.ID_PROV
                LEFT JOIN Establecimientos_E AS ee ON d.ID_DEPTO = ee.ID_DEPTO
                LEFT JOIN Nivel_Educativo_de_ee AS ne ON ee.Cueanexo = ne.Cueanexo
                WHERE ne.id_Nivel_Educativo IN (1, 2, 3, 4)  -- Solo niveles comunes
                GROUP BY d.ID_DEPTO, d.Departamento, p.Provincia
                ORDER BY p.Provincia, Cant_Escuelas_Primaria DESC
                '''
                
ee_por_niv = dd.sql(consultaSQL).df()

#Consulto la población separada en edades correspondientes a los niveles
consultaSQL = '''
            SELECT 
                ID_DEPTO,
                SUM(CASE WHEN Edad BETWEEN 3 AND 5 THEN Poblacion ELSE 0 END) AS Poblacion_Inicial,
                SUM(CASE WHEN Edad BETWEEN 6 AND 12 THEN Poblacion ELSE 0 END) AS Poblacion_Primaria,
                SUM(CASE WHEN Edad BETWEEN 13 AND 18 THEN Poblacion ELSE 0 END) AS Poblacion_Secundaria
            FROM Reporte_Demografico
            GROUP BY ID_DEPTO
'''             
pob_por_niv = dd.sql(consultaSQL).df()

#Uno las consultas
consultaSQL = """
                 SELECT 
                 ee.Provincia,
                 ee.Departamento,
                 ee.Cant_Escuelas_Inicial AS Jardines,
                 pob.Poblacion_Inicial AS "Población Jardin",
                 ee.Cant_Escuelas_Primaria AS Primarias,
                 pob.Poblacion_Primaria AS "Población Primaria",
                 ee.Cant_Escuelas_Secundaria AS Secundarios,
                 pob.Poblacion_Secundaria AS "Población Secundaria"
                 FROM ee_por_niv AS ee
                 INNER JOIN pob_por_niv AS pob
                 ON ee.ID_DEPTO = pob.ID_DEPTO

                      """
Nivel_Ed_por_Prov = dd.sql(consultaSQL).df()

#%%
# SELECCION PRIMERAS Y ULTIMAS 3 FILAS
# muestra las primeras 3 filas, puntos suspensivos y las últimas 3 filas
tabla_1 = pd.concat([Nivel_Ed_por_Prov.head(3), pd.DataFrame([['...'] * Nivel_Ed_por_Prov.shape[1]], columns=Nivel_Ed_por_Prov.columns), Nivel_Ed_por_Prov.tail(3)])


###### EXPORTAR A LATEX #########
# Exportar como tabla de LaTeX con un formato más elegante
latex_table = tabla_1.to_latex(index=False, escape=False, column_format='lcccc', 
                          header=['Provincia', 'Departamento', 'Cantidad EE Inicial', 'Poblacion edad Inicial', 'Cantidad EE Primaria', 'Poblacion edad Primaria', 'Cantidad EE Secundaria', 'Poblacion EE Secundaria'], 
                          caption='Informe por Departamento: Provincia, Cantidad de Escuelas por Nivel Educativo y Habitantes por Edad', 
                          label='tab:informe', 
                          float_format='%.2f')
# Guardar en un archivo
with open('tabla_1.tex', 'w') as f:
    f.write(latex_table)
print("Tabla exportada a 'tabla_1.tex' correctamente.")




#%%% EJERCICIO 2

#Decision: los cc con cap s/d no los contamos como mayor a 100

consultaSQL = """
                SELECT 
                    d.ID_DEPTO,
                    d.Departamento,
                    p.Provincia,
                    COUNT(CASE 
                              WHEN cc.Capacidad != 's/d' AND CAST(cc.Capacidad AS INTEGER) > 100 THEN cc.ID_CC 
                              ELSE NULL 
                          END) AS Cantidad_CC
                FROM 
                    Departamentos d
                JOIN 
                    Provincias p ON d.ID_PROV = p.ID_PROV
                LEFT JOIN 
                    Centros_C cc ON d.ID_DEPTO = cc.ID_DEPTO
                GROUP BY 
                    d.ID_DEPTO, d.Departamento, p.Provincia
                ORDER BY 
                    p.Provincia ASC,
                    Cantidad_CC DESC
              """
depto_CC_100 = dd.sql(consultaSQL).df()

consultaSQL= ''' SELECT 
                 Departamento, 
                 Provincia,
                 Cantidad_CC AS "Cantidad de CC con cap>100"
                 FROM 
                 depto_CC_100
                 '''

depto_CC_100 = dd.sql(consultaSQL).df()
# %%
# muestra las primeras 3 filas, puntos suspensivos y las últimas 3 filas
tabla_2 = pd.concat([depto_CC_100.head(3), pd.DataFrame([['...'] * depto_CC_100.shape[1]], columns=depto_CC_100.columns), depto_CC_100.tail(3)])

######## EXPORTAR A LATEX ###########

# Exportar como tabla de LaTeX con un formato más elegante
latex_table = tabla_2.to_latex(index=False, escape=False, column_format='lcccccccc',  # Asegúrate de que 'cccccccc' coincida con el número de columnas
                          caption='Informe por Departamento: Provincia, Cantidad de Centros Culturales cuya capacidad es mayor a 100 personas.', 
                          label='tab:informe', 
                          float_format='%.2f')
# Guardar en un archivo
with open('tabla_2.tex', 'w') as f:
    f.write(latex_table)
print("Tabla exportada a 'tabla2.tex' correctamente.")



#%%% EJERCICIO 3

'''Para cada departamento, indicar provincia, cantidad de CC, cantidad de EE
(de modalidad común) y población total. Ordenar por cantidad EE
descendente, cantidad CC descendente, nombre de provincia ascendente y
nombre de departamento ascendente. No omitir casos sin CC o EE.'''

# Cantidad de CC, EE y población total por departamento
consultaSQL = """
              SELECT 
                  d.ID_DEPTO,
                  d.Departamento,
                  p.Provincia,
                  (
                      SELECT COUNT(*) 
                      FROM Centros_C cc 
                      WHERE cc.ID_DEPTO = d.ID_DEPTO
                      ) AS Cantidad_CC,
                  (
                      SELECT COUNT(*) 
                      FROM Establecimientos_E ee 
                      WHERE ee.ID_DEPTO = d.ID_DEPTO
                      ) AS Cantidad_EE,
                  (
                      SELECT SUM(Poblacion) 
                      FROM (
                          SELECT DISTINCT Edad, Poblacion
                          FROM Reporte_Demografico rd 
                          WHERE rd.ID_DEPTO = d.ID_DEPTO
                          ) AS sub_rd
                      ) AS Poblacion_Total
              FROM 
                  Departamentos d
              JOIN 
                  Provincias p ON d.ID_PROV = p.ID_PROV
              ORDER BY 
                  Cantidad_EE DESC,
                  Cantidad_CC DESC,
                  p.Provincia ASC,
                  d.Departamento ASC;

              """
Cant_CC_EE_Pob = dd.sql(consultaSQL).df()

#%%% EJERCICIO 4

## DECISION 1: los deptos sin centro cultural no van a aparecer en este DF
## DECISION 2: los centros culturales sin mail no se consideran con dominio
### entonces si hay un depto que ninguno de sus centros tiene mail, no va a
### ser considerado para esta tabla

consulta_dominios = """
       WITH conteo_dominios AS (
           SELECT 
               d.ID_DEPTO,
               p.Provincia,
               d.Departamento,
               LOWER(SPLIT_PART(SPLIT_PART(m.Mail, '@', 2), '.', 1)) AS dominio,  ---indico cual es el dominio
               COUNT(DISTINCT cc.ID_CC) AS cnt
           FROM Departamentos d
           JOIN Provincias p ON d.ID_PROV = p.ID_PROV
           JOIN Centros_C cc ON d.ID_DEPTO = cc.ID_DEPTO
           JOIN Mails m ON cc.ID_CC = m.ID_CC
           WHERE m.Mail IS NOT NULL
           AND m.Mail <> ''                                              ---se excluyen registros que tengan cadenas vacías
           GROUP BY d.ID_DEPTO, p.Provincia, d.Departamento, dominio
           ),
       max_counts AS (
           SELECT 
               ID_DEPTO,
               MAX(cnt) AS max_cnt
           FROM conteo_dominios
           GROUP BY ID_DEPTO
           )
       SELECT 
           dc.Provincia,
           dc.Departamento,
           dc.dominio AS Dominio_mas_frecuente
       FROM conteo_dominios dc
       JOIN max_counts mc
       ON dc.ID_DEPTO = mc.ID_DEPTO AND dc.cnt = mc.max_cnt
       ORDER BY dc.Provincia ASC, dc.Departamento ASC
"""

dominios_cc = dd.sql(consulta_dominios).df()


#%% VIZUALIZACIÓN DE DATOS
#%%% EJERCICIO 1

#cant de CC por provincia
consultaSQL = """
              SELECT p.Provincia, COUNT(cc.ID_CC) AS Cantidad_CC
              FROM Provincias AS p
              LEFT JOIN Departamentos AS d ON p.ID_PROV = d.ID_PROV
              LEFT JOIN Centros_C AS cc ON d.ID_DEPTO = cc.ID_DEPTO
              GROUP BY p.Provincia
              ORDER BY Cantidad_CC DESC
              """
cc_por_provincia = dd.sql(consultaSQL).df()

#cambio los nombres a tierra del fuego y caba para que el gráfico sea más legible
cc_por_provincia['Provincia'] = cc_por_provincia['Provincia'].replace({
    'Tierra del Fuego, Antártida e Islas del Atlántico Sur':'Tierra del Fuego',
    'Ciudad Autónoma de Buenos Aires':'CABA'
})

plt.figure(figsize=(10, 6))
plt.rcParams.update({'font.size': 11})
sns.barplot(data=cc_por_provincia, x='Cantidad_CC', y='Provincia', palette='viridis')
plt.title('Cantidad de Centros Culturales por Provincia')
plt.xlabel('Cantidad de Centros Culturales')
plt.ylabel('Provincia')
plt.show()


#%%% EJERCICIO 2
# Cambio el nombre de Tierra del Fuego, Antártida e Islas del Atlántico Sur para que los graficos sea más legible 
Nivel_Ed_por_Prov['Provincia'] = Nivel_Ed_por_Prov['Provincia'].replace(
    'Tierra del Fuego, Antártida e Islas del Atlántico Sur', 
    'Tierra del Fuego'
)
# Elimino la ciudad autonoma de buenos aires ya que al estar representada con un único departamento no puede ser representada con un boxplot (esta información se encuentra disponible en la primera consulta de sql)
df_filtrado = Nivel_Ed_por_Prov[Nivel_Ed_por_Prov['Provincia'] != 'Ciudad Autónoma de Buenos Aires']

# Calculo el total de las provincias y su orden para ponerlas de mayor a menos según cantidad de escuelas
df_totales = df_filtrado.groupby('Provincia')[['Jardines', 'Primarias', 'Secundarios']].sum()
df_totales['Total_EE'] = df_totales.sum(axis=1)
order_provincias = df_totales.sort_values('Total_EE', ascending=False).index.tolist()

# Transformo a formato largo para graficar
df_long = df_filtrado.melt(
    id_vars=['Provincia', 'Departamento'],
    value_vars=['Jardines', 'Primarias', 'Secundarios'],
    var_name='Grupo_Etario',
    value_name='Cantidad_EE'
)


# Grafico los boxplot
plt.figure(figsize=(35,10))
plt.rcParams.update({'font.size': 25})
sns.boxplot(
    data=df_long, 
    x='Provincia', 
    y='Cantidad_EE', 
    hue='Grupo_Etario', 
    palette='Set2',
    order=order_provincias
)
plt.xticks(rotation=90)
plt.xlabel('Provincia')
plt.ylabel('Cantidad de EE')
plt.title('Distribución de Establecimientos Educativos por Provincia y Grupo Etario')
plt.legend(title='Grupo Etario')
plt.show()


#%%% EJERCICIO 3

consultaSQL = '''SELECT ID_DEPTO, 
                 COUNT(*) AS Cantidad_de_EE 
                 FROM Establecimientos_E
                 GROUP BY ID_DEPTO 
                 ORDER BY Cantidad_de_EE''' 

Cantidad_de_EE = dd.sql(consultaSQL).df()                                                  #agrupo las escuelas por departamento para ver la cantidad en cada depto

Cantidad_de_EE = Cantidad_de_EE.merge(Departamentos, on='ID_DEPTO', how='left')            #agrego ID_PROV para poder agruparlos en provincias
Cantidad_de_EE.drop(columns=['Departamento'], inplace=True)                                #elimino el nombre del departamento
Cantidad_de_EE = Cantidad_de_EE.merge(Provincias, on='ID_PROV', how='left')
                #agrego el nombre de las provincias
# Cambio el nombre de Tierra del Fuego, Antártida e Islas del Atlántico Sur para que los graficos sea más legible 
Cantidad_de_EE['Provincia'] = Cantidad_de_EE['Provincia'].replace(
    'Tierra del Fuego, Antártida e Islas del Atlántico Sur', 
    'Tierra del Fuego'
)
# Elimino la ciudad autonoma de buenos aires ya que al estar representada con un único departamento no puede ser representada con un boxplot
df_filtrado2 = Cantidad_de_EE[Cantidad_de_EE['Provincia'] != 'Ciudad Autónoma de Buenos Aires']

medianas = df_filtrado2.groupby("Provincia")["Cantidad_de_EE"].median().sort_values()    #creo un df con las medianas de la cantidad de dapartamentos por provincia
orden_provincias = medianas.index.tolist()

# grafico
plt.figure(figsize=(20,12))
sns.boxplot(x="Provincia", y="Cantidad_de_EE", data=df_filtrado2, order=orden_provincias, palette="viridis")
plt.xticks(rotation=90)
plt.xlabel("Provincia")
plt.ylabel("Cantidad de EE por Departamento")
plt.title("Distribución de EE por Departamento en cada Provincia")
plt.show()


#%% EJERCICIO 4 

# 1. Población por provincia
pop_depto = Reporte_Demografico.groupby("ID_DEPTO")["Poblacion"].sum().reset_index()                            # Sumo la población por departamento 
pop_depto = pop_depto.merge(Departamentos[["ID_DEPTO", "ID_PROV"]], on="ID_DEPTO", how="left")
pop_prov = pop_depto.groupby("ID_PROV")["Poblacion"].sum().reset_index()                                        # ahora la sumo por provincia
pop_prov = pop_prov.merge(Provincias, on="ID_PROV", how="left")                                                 # Agrego el nombre de la provincia

# 2. Establecimientos Educativos por provincia 
EE_prov = Cantidad_de_EE.groupby("Provincia")["Cantidad_de_EE"].sum().reset_index()                             # uso el conteo hecho anteriormente para saber cuantos EE hay por provincia

# 3. Centros Culturales por provincia
CC_depto = Centros_C.merge(Departamentos[["ID_DEPTO", "ID_PROV"]], on="ID_DEPTO", how="left")
CC_prov = CC_depto.groupby("ID_PROV")["ID_CC"].count().reset_index().rename(columns={"ID_CC": "Cantidad_CC"})  # cuento los CC por provincia
CC_prov = CC_prov.merge(Provincias, on="ID_PROV", how="left")                                                   # Agrego el nombre de la provincia

# 4. Calcular indicadores por mil habitantes a nivel provincial
df_EE = EE_prov.merge(pop_prov, on="Provincia", how="left")                                                     # Uno la información de EE y población (usando el nombre de provincia)
df_EE["EE_por_1000"] = df_EE["Cantidad_de_EE"] / df_EE["Poblacion"] * 1000

# Hago lo mismo para CC
df_CC = CC_prov.merge(pop_prov, on="Provincia", how="left")
df_CC["CC_por_1000"] = df_CC["Cantidad_CC"] / df_CC["Poblacion"] * 1000

# Uno ambos indicadores en un solo DataFrame
df_viz = df_EE.merge(df_CC[["Provincia", "CC_por_1000"]], on="Provincia", how="outer")

# 5. Preparo los datos para el gráfico por provincia (dos barras por provincia)
df_long = pd.melt(df_viz, id_vars="Provincia", 
                  value_vars=["EE_por_1000", "CC_por_1000"],
                  var_name="Tipo", value_name="Valor")
# cambio los nombres
df_long["Tipo"] = df_long["Tipo"].map({
    "EE_por_1000": "Establecimientos Educativos",
    "CC_por_1000": "Centros Culturales"
})

# cambio los nombres a tierra del fuego y caba para que el gráfico sea más legible
df_long['Provincia'] = df_long['Provincia'].replace({
    'Tierra del Fuego, Antártida e Islas del Atlántico Sur':'Tierra del Fuego',
    'Ciudad Autónoma de Buenos Aires':'CABA'
})

# Gráfico por provincia
plt.figure(figsize=(25,15))
sns.barplot(data=df_long, x="Provincia", y="Valor", hue="Tipo", palette="Set2")
plt.xticks(rotation=90)
plt.xlabel("Provincia")
plt.ylabel("Cantidad por mil habitantes")
plt.yscale("log")
plt.title("Centros Culturales vs Establecimientos Educativos por mil habitantes por Provincia")
plt.grid(True)
plt.show()

# 6. Calculo los indicadores a nivel nacional
total_EE = EE_prov["Cantidad_de_EE"].sum()
total_CC = CC_prov["Cantidad_CC"].sum()
total_pop = pop_prov["Poblacion"].sum()

EE_por_1000_nacional = total_EE / total_pop * 1000
CC_por_1000_nacional = total_CC / total_pop * 1000

df_nacional = pd.DataFrame({
    "Tipo": ["Establecimientos Educativos", "Centros Culturales"],
    "Valor": [EE_por_1000_nacional, CC_por_1000_nacional]
})

# Gráfico nacional
plt.figure(figsize=(20,15))
sns.barplot(data=df_nacional, x="Tipo", y="Valor", palette="Set2")
plt.xlabel("")
plt.ylabel("Cantidad por mil habitantes")
plt.title("Comparación Nacional: Centros Culturales vs Establecimientos Educativos por mil habitantes")
plt.yscale('log')
plt.grid()
plt.show()
