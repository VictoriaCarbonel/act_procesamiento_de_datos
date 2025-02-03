
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



































