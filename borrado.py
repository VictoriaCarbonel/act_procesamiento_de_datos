# %% Creo una imagen de la tabla

data = Nivel_Ed_por_Prov.head().values.tolist()

# Cre0 la fila de ("⋮") para cada columna
ellipsis_row = ["⋮" for _ in Nivel_Ed_por_Prov.columns]
data.append(ellipsis_row)

fig, ax = plt.subplots(figsize=(6, 1))

tabla = ax.table(cellText=data,
                 colLabels=Nivel_Ed_por_Prov.columns,
                 cellLoc='center',
                 loc='center')

# Ajusto los márgenes
plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

# Cambio el fondo de los encabezados
for (fila, col), celda in tabla.get_celld().items():
    if fila == 0:
         celda.set_facecolor('lightgray') 

# Desactivo los ejes
ax.axis('off')
plt.savefig('Nivel_Ed_por_Prov.png', bbox_inches='tight', dpi=300)

plt.show()

# %% PRUEBA PARA EXPORTAR TABLA a LATEX
# Exportar a LaTeX con formato personalizado
latex_table = ee_por_niv.head().to_latex(index=False, escape=False, 
    column_format='|l|l|c|c|c|c|c|c|', 
    multirow=False, 
    longtable=False)

# Agregar encabezados y configuración adicional
latex_table = f"""
\\begin{{table}}[H]
    \\centering
    \\small % Reduce el tamaño de la fuente
    \\renewcommand{{\\arraystretch}}{{1.2}} % Ajuste del espaciado entre filas
    \\setlength{{\\tabcolsep}}{{4pt}} % Reduce el espacio entre columnas
    \\resizebox{{\\textwidth}}{{!}}{{ % Ajusta la tabla al ancho de la página
    {latex_table}
    \\hline
    }}
    \\caption{{Distribución de establecimientos educativos y población por nivel educativo.}}
    \\label{{tab:reporte_departamentos}}
\\end{{table}}
"""

# Guardar en un archivo .tex
with open('tabla.tex', 'w') as f:
    f.write(latex_table)

print("La tabla se ha exportado a 'tabla.tex'.")

# %%

# %%

