
#Actividad 1

"""Deben generar una matriz (de 3 filas x 4 columnas) denominada empleado_01
conteniendo los siguientes datos (todos de tipo entero)
Importante: Implementar la matriz como lista de filas"""

import numpy as np

empleado_01 = np.array([[20222333,45,2,20000],[33456234,40,0,25000],[45432345,41,1,10000]])


"""Programar la función superanSalarioActividad01 en py y que devuelva una nueva 
matriz (con las 4 columnas) conteniendo aquellos empleados que ganan un 
salario > umbral. Solo está permitido usar ciclos (while/for), condicionales(if), etc.
No está permitido usar funciones de bibliotecas"""


def superanSalarioActividad01(empleado_01,umbral):
    nueva_matriz=[]
    
    for fila in empleado_01:  #Costo 0(n), recorro la matriz n veces, su longitud
        if fila[3]>= umbral:
            nueva_matriz.append(fila) #Costo 0(1) porque lo agrego atrás?
    
    print(nueva_matriz)
    
    
superanSalarioActividad01(empleado_01, 15000)


"""¿Cuánto les costó implementar la función? """

-------------------------------------------------------------------------------

#Actividad 2


"""¿Qué pasa si se agregan más filas a la matriz? Probar con la nueva matriz empleado_02
¿La función superanSalarioActividad01(empleado_02, 15000) continua funcionando?"""

import numpy as np

empleado_02 = np.array([[20222333,45,2,20000],[33456234,40,0,25000],[45432345,41,1,10000], [43967304,37,0,12000],[42236276,36,0,18000]])

def superanSalarioActividad01(empleado,umbral):
    nueva_matriz=[]
    
    for fila in empleado:  #Costo 0(n), recorro la matriz n veces, su longitud
        if fila[3]>= umbral:
            nueva_matriz.append(fila) #Costo 0(1) porque lo agrego atrás?
    
    print(nueva_matriz)
    
    
superanSalarioActividad01(empleado_02, 15000)

#Rta: [array([20222333, 45, 2, 20000]), array([33456234, 40, 0, 25000]), array([42236276, 36, 0, 18000])]

"""Agregar más filas a la matriz no afecta el desempeño de la función, sigue
cumpliendo con su cometido"""


-------------------------------------------------------------------------------

#Actividad 3

"""¿Qué pasa si se modifica el orden de las columnas de la matriz? Probar con la 
nueva matriz empleado_03

¿La función superanSalarioActividad01 sigue funcionando? En caso de no ser asi
implementar una nueva función superanSalarioActividad03, que tome como entrada 
la matriz empleado_03 y un valor entero denominado umbral, y que devuelva aquellos
empleados que ganan un salario > umbral. El orden de las columnas debe ser el original"""



-------------------------------------------------------------------------------

#Actividad 4

"""¿Qué pasa si a la matriz de entrada se la implementa como lista de columnas en
vez de lista de filas? Probar con la nueva matriz empleado_04. La matriz resultante
debe seguir implementada como lista de filas

¿Alguna de las funciones anteriores (superanActividad0...) funciona? En caso de no
funcionar implementar una nueva función superanActividad04, que tome como entrada 
la matriz empleado_04 y un valor entero denominado umbral, y que devuelva aquellos
empleados que ganan un salario > umbral. El orden de las colmunas es el original"""




---------------------------------------------------------------------------------

#Actvidad 5

"""¿Cómo afecto a la programación de la función cuando cambiaron levemente a matriz
empleado?
    a. En el caso en que le agregaron más filas
    b. En el caso en que le alteraron el orden de las columnas"""



"""¿Y cuando a empleado le cambiaron la forma de representar las matrices (de lista 
de filas a lista de columnas)?"""
    
    
    
    
"""¿Cuál es la ventaja, desde el punto de vista del usuario de la función, disponer
de ella y no escribir directamente el código de la consulta dentro de su programa?"""




























