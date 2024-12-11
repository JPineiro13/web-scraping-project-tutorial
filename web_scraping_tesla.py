#web scraping

# Importo las librerías
import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import matplotlib.pyplot as plt
from datetime import timedelta

url = "https://ycharts.com/companies/TSLA/revenues"


def web_scrape_de_tesla(url):

# Resulta que me estaba bloqueando el acceso porque no soy un navegador, sino un programa de python.
# Lo que tengo que hacer es pretender que soy un navegador con esto:
# "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36
# Esto es una cabecera. Cuando los navegadores entran, dan estos datos para indicar quiénes son. 
# Si doy estos datos, me haré pasar por un navegador
	
	cabecera = {

	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"

	}

	#Con request.get le pido conexión a la url. Si me responde 200 es que todo ha ido bien.
	#Y ahora hay que indicarle que use la cabecera al hacer la petición
	respuesta = requests.get(url, headers=cabecera)

	print(respuesta)

	if respuesta.status_code != 200:
		print("Algo ha ido mal")

	else: 
		print("Ha respondido 200. Todo bien")


		#Ahora que ya me han abierto la puerta paso al punto 3 del beautiful soup

		#Me creo esta variable que va a ser donde esten todos los datos ya estructurados
		#Alguien se ha molestado en crear html.parser y ahora puedo decirle a BeautifulSoup que tome la parte .text de la respuesta
		#que he obtenido y le aplique la transformación html.parser, que lo dejará más ordenado.

		soup = BeautifulSoup(respuesta.text, "html.parser")
		#print(soup)


		#Ahora hay que buscar todas las tablas

		tablas = soup.find_all("table")
		#print(tablas)

		#Ahora tengo que coger solo la tabla trimestral, que es la primera. Por tanto:
		tabla_trimestral = tablas[1]
		print("_____________________SACO LA TABLA TRIMESTRAL_____________________")
		print(tabla_trimestral)


		#Estoy buscando concretamente la información que está en table row, entonces pongo "tr"
		filas = tabla_trimestral.find_all("tr")
		# Es decir, me trae todo lo que tenga el tag/la etiqueta "tr" en html
		print("_____________________SACO SOLO LAS FILAS_____________________")
		print(filas)

		#Esta info es lo que tengo que meter al dataframe
		datos = []
		#No necesito los datos de la cabecera
		for row in filas [1:]:
			columnas = row.find_all("td")  #td es de table data. Además, hay que usar obligatoriamente "row" para buscar las filas
			#Como tiene que haber dos columnas, una con fecha y otra con valor, lo que hago es traerme esas dos y estructurarlas
			if len(columnas) == 2:
				fecha = columnas[0].text.strip()
				ingresos = columnas[1].text.strip()
				datos.append([fecha, ingresos])

			print("_____________________SACO FECHA E INGRESOS_____________________")
			print(datos)

		# Ahora ya tengo en datos[] solo la información que necesito para poder manipularlo ---> fecha e ingresos
		#Ahora meto esta información de datos en un dataframe:
		df = pd.DataFrame(datos, columns=["Fecha", "Ingresos"])

		# Nos piden que limpiemos quitando tanto $ como casillas vacías, pero en realidad no hay ningún $, así que no hace falta quitarlos 
		#df["Ingresos"] = df["Ingresos"].replace('[\$,]', '', regex=True)
		df = df[df["Ingresos"] != ""] #Es deicr, conservo lo que no esté vacío

		print("_____________________PRINTEO EL DF_____________________")
		print(df)

		#Esto, a pesar de estar en mitad del código, es lo último que he añadido.
		#Al graficar más adelante he visto que saca el orden de las fechas de más nuevo a más antiguo, lo cual es raro visualmente
		#Aquí ordeno las fechas para que luego las saque bien
		df['Fecha'] = pd.to_datetime(df['Fecha'])
		df = df.sort_values(by='Fecha')

		print("_____________________PRINTEO EL DF ORDENADO_____________________")
		print(df)

		#El quinto apartado me dice que me cree una base de datos SQLite y que meta ahí (en una tabla) la información del df.
		#La idea es que luego traiga los datos de esa tabla para sacar gráficos etc etc

		#Ya he probado a instalar el sqlite en local. Resulta que ya venía por defecto, como el homebrew.
		#Por tanto ya tengo el motor sqlite en local. Dentro de ese seridor de bases de datos voy a crear las bases de datos que necesite.
		#Dentro de esas bases de datos crearé las tablas que voy a usar.

		#Lo primero que tengo que hacer es conectarme al servidor de bases de datos. Al conectarme ya se crea la base de datos
		print("_____________Me conecto a la base de datos_____________")
		conexion_a_la_db = sqlite3.connect('ingresos_tesla.db')
		comando_sql = conexion_a_la_db.cursor()

		#Ahora, dentro de esta base de datos, creo la tabla
		print("_____________Creo la tabla_ingresos_tesla_____________")
		comando_sql.execute('''
		CREATE TABLE IF NOT EXISTS tabla_ingresos_tesla (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			fecha TEXT,
			ingresos TEXT
			)
			''')

		# Guardar datos en la base de datos
		print("_____________Cargo los datos desde el df a la tabla_ingresos_tesla de la db_____________")
		df.to_sql('tabla_ingresos_tesla', conexion_a_la_db, if_exists='replace', index=False)
		print("_____________Datos del df guardados en la tabla_ingresos_tesla de la db._____________")

		# Le pido ls datos de la tabla
		query = "SELECT fecha, ingresos FROM tabla_ingresos_tesla"
		df = pd.read_sql_query(query, conexion_a_la_db)

		# Y ahora hay que corregir el formato en que aparecen los números porque 45.0M o 4.02B no se puede usar directamente
		#Hay que eliminar las letras y mantener las proporciones entre los números. Además hay que convertirlo en float
		# En realidad no hace tanta falta pasar los billones a billones como mantener su proporción con los millones.
		#Como la web es americana, sus billones son 1000 veces más grandes que los millones.
		#Si la página fuera europea entonces los números cambiarían, y los billones serían en realidad 1.000.000 millones
		def convertir_ingresos(valor):
			if 'B' in valor:
				return float(valor.replace('B', '')) * 1000
			elif 'M' in valor:
				return float(valor.replace('M', ''))
			else:
				return float(valor)

		#Convierto los ingresos aplicando las transformaciones anteriores
		df['Ingresos'] = df['Ingresos'].apply(convertir_ingresos)


		#Como tooooodo esto está dentro del else, estas partes solamente se van a ejecutar si todo ha ido bien
		#Esto es un gráfico estandar en el que tomo los datos de fecha e ingresos que ya tengo
		plt.figure(figsize=(10, 6))
		plt.plot(df['Fecha'], df['Ingresos'], marker='o', color='blue')
		plt.title('Ingresos Trimestrales de Tesla', fontsize=14)
		plt.xlabel('Fecha', fontsize=12)
		plt.ylabel('Ingresos (Millones)', fontsize=12)
		plt.xticks(rotation=45)
		plt.grid()
		plt.tight_layout()
		plt.show()

		# Aquí calculo la diferencia de un trimestre a otro. Como a Tesla le está yendo muy bien de forma 
		# sostenida en el tiempo, deberíamos encontrar números positivos la mayoría del tiempo.
		df['Crecimiento Intertrimestral'] = df['Ingresos'].diff()

		# Y lo ploteo
		plt.figure(figsize=(10, 6))
		plt.bar(df['Fecha'], df['Crecimiento Intertrimestral'], color='orange', alpha=0.7)
		plt.title('Crecimiento Intertrimestral', fontsize=14)
		plt.xlabel('Fecha', fontsize=12)
		plt.ylabel('Crecimiento (Millones)', fontsize=12)
		plt.xticks(rotation=45)
		plt.grid(axis='y', linestyle='--', alpha=0.7)
		plt.tight_layout()
		plt.show()

		#Y ahora introduzco aquí un concepto inventado: El inversor triste
		# Como inversor, al ver gráficas a veces me pregunto por aquellas personas que habrán invertido en x cosa justo antes de que pegasen una gran bajada
		# Según el tipo de activo, muchas veces estar en máximos históricos no es tan peligroso, pero hay otras que se tarda mucho en remontar
		# El inversor triste para mí es aquel que tiene el peor timing posible, y la idea de esta métrica es la de medir el peor escenario posible
		# El peor escenario posible es que los números no remonten nunca, lo cual no pasa en esta gráfica. 
		# En caso de remontar, el peor escenario será aquel en el que esa remontada se demore lo más posible.
		
		# Calcular el tiempo en trimestres que cada ingreso tarda en ser superado
		espera_maxima = 0  # Mayor distancia (en trimestres)
		peor_timing = None  # Índice del trimestre con mayor espera
		indice_remontada = None  # Índice del trimestre donde se supera el ingreso

		for i in range(len(df) - 1):  # No hay ningún trimestre después del útlimo (sorprendentemente), por lo que no hay que analizar el útlimo
			ingresos_actuales = df.loc[i, 'Ingresos']
		
			# Busco el primer trimestre posterior que supere los ingresos
			for j in range(i + 1, len(df)):
				if df.loc[j, 'Ingresos'] > ingresos_actuales:
					distancia = j - i  
				if distancia > espera_maxima:
						espera_maxima = distancia
						peor_timing = i
						indice_remontada = j
						break  

		if peor_timing is not None and indice_remontada is not None:
			print(f"El accionista más triste habría comprado en {df.loc[peor_timing, 'Fecha']} y habría esperado {espera_maxima} trimestres hasta {df.loc[indice_remontada, 'Fecha']} para ver ingresos superiores.")
		else:
			print("No se encontró ningún trimestre en el que los ingresos fueran superados.")

		# Graficar los ingresos y marcar el punto del accionista triste
		plt.figure(figsize=(10, 6))
		plt.plot(df['Fecha'], df['Ingresos'], marker='o', color='blue', label='Ingresos')
		plt.axvline(x=peor_timing-1, color='red', linestyle='--', label='Accionista triste')
		plt.title('Ingresos Trimestrales de Tesla y Accionista Triste', fontsize=14)
		plt.xlabel('Fecha', fontsize=12)
		plt.ylabel('Ingresos (Millones)', fontsize=12)
		plt.xticks(rotation=45)
		plt.legend()
		plt.grid()
		plt.tight_layout()
		plt.show()

		#Y, por supuesto, tengo que cerrar la conexión. Cerrarla antes de haber terminado 
		#las peticiones habría sido un error...
		conexion_a_la_db.close()

#Llamo al programa principal en el que he incluído todo
web_scrape_de_tesla(url)


#Ya sé que no hay que confundir los ingresos con el precio de las acciones. Tan sólo estaba buscando un indicador del desempeño de la empresa
#Pero no pretendo decir que son lo mismo.
#Por otra parte, he tratado de implementar otras métricas interesantes como:
# - Qué porcentaje de inversores ven crecimiento al trimestre siguiente?
# - Qué porcentje de inversores ven crecimiento dos trimestres después? 
# - Etc etc

#La idea de esta métrica es analizar qué cantidad de tiempo es esperable para obtener beneficio, así como cuál es recomendable

#Falta de tiempo, paso al siguiente proyecto. Te veo allí!








