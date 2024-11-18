import pandas as pd

archivo_excel = "C:/Users/hrond/OneDrive/Escritorio/Analytics/Atenciones.xlsx"
df = pd.read_excel(archivo_excel)

print(df.head())
# Eliminar filas con valores nulos
df = df.dropna()
print(df.head())
print("p")
