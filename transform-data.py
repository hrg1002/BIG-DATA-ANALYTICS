# Description: Transform data from clinical_data.csv to spark dataframe
import datos_clinicos_ad 

# Załaduj dane kliniczne
clinical_data = datos_clinicos_ad.df

# Wyświetl pierwsze 100 wierszy
# Utwórz sesję Spark

# Uzyskaj tylko drugą i trzecią wiersz
filtered_data = clinical_data.iloc[0:2]
transposed_data = filtered_data.transpose() 
# Wyodrębnij odpowiednie kolumny i zmień ich nazwy
transposed_data.columns = ['years', 'total_cases']
transposed_data.drop(transposed_data.index[0], inplace=True)
print(transposed_data)
medical_cleaned_data = transposed_data
# Wyświetl dane
#transform weather and air pollution data
weather_data = 
