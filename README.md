# trafico-bogota
#Análisis en batch y tiempo real del tráfico vehicular en Bogotá.
# Función para clasificar nivel de congestión basada en velocidad promedio
def clasificar_congestion(df, velocidad_col='velocidad_promedio'):
return df.withColumn(
'nivel_congestion',
when(df[velocidad_col] >= 30, 'Bajo')
.when((df[velocidad_col] >= 15) & (df[velocidad_col] < 30), 'Medio')
.otherwise('Alto')
)

