"""
Importamos la libreria pymongo y su modulo MongoClient para establecer una conexion con Mongo
Importamos la libreria pprint para mostrar por pantalla los documentos mas ordenados
Importamos la libreria json para trabajar con los archivos JSON que importamos
Importamos la libreria requests para hacer una request a una API 
Importamos la libreria pandas para poder mostrar como tabla los documentos
Importamos la libreria matplotlib y su modulo pyplot para visualizar graficos
Importamos la libreria numpy para crear arrays
"""
import pymongo
from pymongo import MongoClient
import pprint
import json
import requests
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

#Creamos un objeto cliente de la clase MongoClient en el cual indicamos que la conexion va a ser en local (localhost)
cliente=MongoClient("localhost")

#Creamos una BBDD a la que le damos un nombre
bbdd=cliente["BBDD_Ignacio_Dorado"]

#Creamos una coleccion a la que le damos un nombre
coleccion=bbdd["Residentes_Barrios_Madrid"]

#Creamos un objeto printer de la clase PrettyPrinter para utilizar la impresion de manera mas ordenada
printer=pprint.PrettyPrinter()

#------------------------------POR CARGA DE ARCHIVO----------------------------------------------------------

"""
Link del archivo para su descarga
https://datos.gob.es/en/catalogo/a13002908-residentes-en-la-comunidad-de-madrid-por-rango-de-edad1

"""

#Creamos una funcion para cargar el archivo JSON de nuestro directorio a Python y nos lo devuelva en un diccionario
def archivo_descargado():
    
    #Abrimos el archivo JSON a utilizar con la funcion open
    with open("madrid_gente.json") as archivo_json:
        
        #Cargamos el archivo JSON a una variable como diccionario
        file=json.load(archivo_json)

    #La funcion nos devuelve la variable con el diccionario en su interior
    return file

#------------------------------POR REQUEST A UNA API-----------------------------------------------------------

"""
Link de la API donde vamos a hacer el rquest
https://datos.gob.es/en/apidata#!/dataset/findDatasetsbyPublisher

URL de acceso dentro del JSON que genera la API
"accessURL": "https://datos.comunidad.madrid/catalogo/dataset/38565f9e-5bc3-42b7-b128-4f2bcb3cafa9/resource/fa55eac4-b3f0-47b1-a670-3920b88338d0/download/madrid.json",

"""

#Creamos una funcion para realizar una request a una API y que nos devuelva el archivo JSON en diccionario
def api_request():
    
    #URL para la API
    url="https://datos.comunidad.madrid/catalogo/dataset/38565f9e-5bc3-42b7-b128-4f2bcb3cafa9/resource/fa55eac4-b3f0-47b1-a670-3920b88338d0/download/madrid.json"
    
    #Hacemos request a la API
    variable=requests.get(url)

    #Comprobamos el estado de la conexion, si es bueno realizamos la carga del contenido
    if variable.ok:

        #Cargamos el contenido JSON a una variable como diccionario
        file=json.loads(variable.content)

        #Esta funcion tambien nos devuelve la variable con el diccionario en su interior
        return file

    #Si la conexion no es buena no realiza la carga del archivo
    else:

        #La funcion no devuelve nada
        return 0

#Creamos una funcion para añadir el diccionario con los registros en nuestra coleccion
def annadir(tipo):

    #Indicamos la variable global para utilizar la coleccion en el resto de funciones
    global coleccion

    #Si la opcion que le indicamos es por la carga de archivo
    if tipo.lower()=="carga":

        #LLamamos a la funcion de carga de archivo que nos devuelve el diccionario con los registros
        registros=archivo_descargado()

    #Si la opcion que le indicamos es la request a la API
    elif tipo.lower()=="api":

        #LLamamos a la funcion de la request a la API que nos devuelve el diccionario con los registros
        registros=api_request()

    #Si la opcion seleccionado no es ninguna de las anteriores no llama a ninguna funcion
    else:

        #La funcion no devuelve nada
        return 0

    #Obtenemos la lista de registros (documentos clave valor) de la clave "data" del diccionario
    lista_rangos=registros["data"]

    #Iteramos a traves de la lista de registros
    for i in lista_rangos:

        #Añadimos cada uno de los registros a nuestra coleccion utilizando la funcion insert_one
        coleccion.insert_one(i)

    #Mostramos por pantalla que la insercion de los datos ha sido exitosa
    print("Registros insertados con exito")

"""
Llamada a la funcion de añadir todos los registros a nuestra coleccion
Es necesario pasarle la opcion de cargado de los datos (carga o api)
"""
#annadir("api")

#Creamos una funcion para eliminar todos los registros de nuestra coleccion
def eliminar_todo():

    #Eliminamos todos los registros utilizando la funcion delete_many pasandole un diccionario vacio
    coleccion.delete_many({})

    #Mostramos por pantalla que la eliminacion de todos los datos ha sido exitosa
    print("Eliminado con exito")


#Llamada a la funcion de eliminar todos los documentos de nuestra coleccion
#eliminar_todo()

#Creamos una funcion para mostrar todos los registros (documentos) de nuestra coleccion
def ver_todo():

    #Indicamos la variable global para utilizar el objeto de la clase PrettyPrinter
    global printer

    #Utilizamos la funcion find para almacenar todos los registros en una variable pasandole un diccionario vacio
    todos_registros=coleccion.find({})

    #Iteramos a traves del objeto
    #for i in todos_registros:

        #Mostramos por pantalla cada documento de nuestra coleccion
        #printer.pprint(i)

    #Para verlo en modo tabla, pasamos la variable en forma de lista a la clase DataFrame de la libreria de pandas
    df=pd.DataFrame(list(todos_registros))

    #Mostramos por patalla el dataframe
    print(df)

#Llamada a la funcion de visualizar todos los documentos de nuestra coleccion 
#ver_todo()

#Creamos una funcion para que nos cuente todos los documentos que hay en nuestra coleccion
def documentos_totales():

    #Utilizamos la funcion count_documents pasandole un diccionario vacio que nos devuelve el numero de documentos
    total=coleccion.count_documents({})

    #Mostramos por pantalla el numero de documentos de nuestra coleccion
    print(f"El numero de documentos de este dataset es de {total}")


#Llamada a la funcion para conocer el numero de documentos de nuestra coleccion
#documentos_totales()

#Creamos una funcion que nos permite actualizar o desactualizar un campo concreto de nuestra coleccion
def actualizar_desactualizar(mi_barrio):

    #Si la opcion de actualizar a mi barrio es la seleccionada (True)
    if mi_barrio:

        #Actualiza utilizando la funcion update_many los documentos cuyo nombre del barrio sea Prosperidad por Mi barrio
        coleccion.update_many({"barrio_nombre":"Prosperidad"}, {"$set":{"barrio_nombre": "Mi barrio"}})

    #Si la opcion de actualizar a mi barrio no es la seleccionada (False)
    else:

        #Actualiza (desactualiza) utilizando la funcion update_many los documentos cuyo nombre del barrio sea Mi barrio por Prosperidad
        coleccion.update_many({"barrio_nombre":"Mi barrio"}, {"$set":{"barrio_nombre": "Prosperidad"}})

    #Utilizamos la funcion find para almacenar todos los registros en una variable pasandole un diccionario con el codigo del barrio
    prospe=coleccion.find({"barrio_codigo":"0796052"})

    #Iteramos a traves del objeto
    for i in prospe:

        #Mostramos por pantalla los documentos obtenidos de nuestra coleccion
        printer.pprint(i)

"""
Llamada a la funcion para actualizar documentos de nuestra coleccion
Es necesario pasarle la opcion de actualizacion (True o False) 
"""
#actualizar_desactualizar(True)

#Creamos una funcion que nos permite buscar por el _id en nuestra coleccion
def documentos_id(cadena_id):

    #Importamos el modulo ObjectId de la libreria bson (JSON binario)
    from bson.objectid import ObjectId

    #Creamos una objeto ObjectId pasandole el string del _id del registro
    objeto_id=ObjectId(cadena_id)

    #Utilizamos la funcion find_one para buscar un unico documento a traves de su _id
    registro_id=coleccion.find_one({"_id":objeto_id})

    #Mostramos por pantalla el documento obtenido
    printer.pprint(registro_id)

"""
Llamada a la funcion para filtar documentos de nuestra coleccion por su id
Es necesario pasarle el id en string 
"""
#documentos_id("6357a049124b3cc6e76fb9a4")

#Creamos una funcion para buscar documentos segun el sexo, el barrio y el rango de edad
def sexo_barrio_edad(sexo, barrio, edad1, edad2):

    #Utilizamos la funcion find para almacenar todos los registros en una variable pasandole las condiciones indicadas
    muj_buenav=coleccion.find({"$and":[{"sexo":f"{sexo.title()}"},{"barrio_nombre":f"{barrio.title()}"},{"rango_edad":{"$regex":f"De {edad1} a {edad2}"}}]},
                            {"barrio_nombre":1, "sexo":1, "rango_edad":1, "poblacion_empadronada":1,"_id":0})

    #Iteramos a traves del objeto
    for i in muj_buenav:

        #Mostramos por pantalla los documentos obtenidos de nuestra coleccion
        printer.pprint(i)

"""
Llamada a la funcion para filtar documentos segun el sexo, el barrio y el rango de la edad
Es necesario pasarle el sexo, el nombre del barrio, la edad inferior y superior del rango
"""
#sexo_barrio_edad("mujer", "buenavista", 20, 24)

#Creamos una funcion para buscar documentos por el codigo del barrio
def cod_barrio(codigo):

    #Utilizamos la funcion find para almacenar todos los registros en una variable pasandole el codigo del barrio
    codigobarrio=coleccion.find({"barrio_codigo": codigo},
                                {"barrio_nombre":1, "sexo":1, "rango_edad":1, "poblacion_empadronada":1,"distrito":1 ,"_id":0})

    #Iteramos a traves del objeto
    for i in codigobarrio:

        #Mostramos por pantalla los documentos obtenidos de nuestra coleccion
        printer.pprint(i)

"""
Llamada a la funcion para filtar documentos segun el codigo del barrio
Es necesario pasarle el codigo del barrio en string
"""
#cod_barrio("0796022")

#Creamos una funcion para buscar documentos cuya gente empadronada este dentro de un rango
def empadronada_rango(n1, n2):

    #Comprobamos que el limite inferior no es superior al limite superior
    if n1>n2:

        #La funcion no devuelve nada 
        return 0

    #Utilizamos la funcion find para almacenar todos los registros en una variable pasandole el rango que queremos
    documentos_cumplen=coleccion.find({"$and":[{"poblacion_empadronada":{"$gte":n1}},{"poblacion_empadronada":{"$lte":n2}}]},
                                        {"barrio_nombre":1, "poblacion_empadronada":1,"rango_edad":1,"_id":0})
    
    #Iteramos a traves del objeto
    for i in documentos_cumplen:

        #Mostramos por pantalla los documentos obtenidos de nuestra coleccion
        printer.pprint(i)

"""
Llamada a la funcion para filtar documentos segun el rango de gente empadronada
Es necesario pasarle el limite inferior y superior del rango
"""
#empadronada_rango(0, 10)

#Creamos una funcion que nos diga toda la gente que hay empadronada en Madrid
def gente_madrid():

    #Etapa para agrupar todos los documentos y sumar la poblacion empadronada
    etapa1={"$group":{"_id":None, "gente":{"$sum":"$poblacion_empadronada"}}}

    #Lista con las etapas del pipeline
    pipeline=[etapa1]

    #Utilizamos la funcion aggregate para realizar las etapas y convertir a lista lo que devuelva
    gente_mad=list(coleccion.aggregate(pipeline))

    #Recogemos, del indice 0 de la lista y la clave "gente", el valor
    cantidad_gente=gente_mad[0]["gente"]

    #Mostramos por pantalla el numero de personas empadronadas en Madrid
    print(f"El numero de empadronados en Madrid es de {cantidad_gente} personas")


#Llamada a la funcion para obtener el total de empadronados en Madrid
#gente_madrid()

#Creamos una funcion que nos diga el distrito que tiene mas personas empadronadas
def distrito_mas_personas():

    #Etapa para agrupar los documentos por el distrito y sumar la poblacion empadronada
    etapa1={"$group":{"_id":"$distrito_nombre", "gente":{"$sum":"$poblacion_empadronada"}}}

    #Etapa para ordenar por el campo gente de manera descendente
    etapa2={"$sort":{"gente":-1}}

    #Etapa para limitar el numero de documentos a 1
    etapa3={"$limit":1}

    #Lista con las etapas del pipeline
    pipeline=[etapa1, etapa2, etapa3]

    #Utilizamos la funcion aggregate para realizar las etapas y convertir a lista lo que devuelva
    gente_en_distrito=list(coleccion.aggregate(pipeline))

    #Recogemos, del indice 0 de la lista y la clave "id", el valor
    distrito=gente_en_distrito[0]["_id"]

    #Recogemos, del indice 0 de la lista y la clave "gente", el valor
    cantidad=gente_en_distrito[0]["gente"]

    #Mostramos por pantalla el distrito con mas personas empadronadas y su cantidad
    print(f"El distrito con mas empadronados es {distrito} con {cantidad} personas")


#Llamada a la funcion para obtener el distrito con mas empadronados
#distrito_mas_personas()

#Creamos una funcion que nos indique todos los barrios de Madrid en orden alfabetico
def barrios_orden():

    #Etapa para agrupar los documentos por el barrio
    etapa1={"$group":{"_id":"$barrio_nombre"}}

    #Etapa para ordenar de manera ascente (alfabetico)
    etapa2={"$sort":{"_id":1}}

    #Lista con las etapas del pipeline
    pipeline=[etapa1, etapa2]

    #Utilizamos la funcion aggregate para realizar las etapas y convertir a lista lo que devuelva
    barrios=list(coleccion.aggregate(pipeline))

    #Iteramos a traves de la lista
    for i in barrios:

        #Mostramos por pantalla los valores de la clave _id de cada elemento de la lista
        print(i["_id"])

#Llamada a la funcion para obtener todos los barrios de Madrid por orden alfabetico
#barrios_orden()

#Creamos una funcion que nos indique la cantidad de rangos de edad que hay 
def rangos():

    #Etapa para agrupar los documentos por el rango de edad
    etapa1={"$group":{"_id":"$rango_edad"}}

    #Etapa para agrupar todos los documentos y sumarlos
    etapa2={"$group":{"_id":None, "suma": {"$sum":1}}}

    #Lista con las etapas del pipeline
    pipeline=[etapa1, etapa2]

    #Utilizamos la funcion aggregate para realizar las etapas y convertir a lista lo que devuelva
    rangos=list(coleccion.aggregate(pipeline))

    #Recogemos, del indice 0 de la lista y la clave "suma", el valor
    cantidad=rangos[0]["suma"]

    #Mostramos por pantalla el numero de rangos de edad que hay
    print(f"El numero de rangos de edad que hay es de {cantidad}")


#Llamada a la funcion para obtener la cantidad de rangos de edad que hay
#rangos()

#Creamos una funcion que nos indique cuanta gente hay empadronada en un barrio concreto
def gente_por_barrio(barrio):

    #Etapa para filtrar los documentos por el nombre del barrio
    etapa1={"$match":{"barrio_nombre":f"{barrio.title()}"}}

    #Etapa para agrupar los documentos por el barrio y sumar la poblacion empadronada
    etapa2={"$group":{"_id":"$barrio_nombre", "gente":{"$sum":"$poblacion_empadronada"}}}

    #Lista con las etapas del pipeline
    pipeline=[etapa1, etapa2]

    #Utilizamos la funcion aggregate para realizar las etapas y convertir a lista lo que devuelva
    gente_en_barrio=list(coleccion.aggregate(pipeline))

    #Recogemos, del indice 0 de la lista y la clave "_id", el valor
    barrio=gente_en_barrio[0]["_id"]

    #Recogemos, del indice 0 de la lista y la clave "gente", el valor
    cantidad=gente_en_barrio[0]["gente"]

    #Mostramos por pantalla el barrio y la cantidad de gente empadronada que tiene
    print(f"El numero de empadronados en {barrio} es de {cantidad} personas")

"""
Llamada a la funcion para obtener la cantidad de gente empadronada en un barrio concreto
Es necesario pasarle el nombre del barrio
"""
#gente_por_barrio("buenavista")

#Creamos una funcion para saber el top 5 de los barrios con mas o menos gente empadronada por el sexo indicado
def top5_barrios_orden_sexo(orden, sexo):

    #Etapa para filtrar los documentos por el sexo
    etapa1={"$match":{"sexo":f"{sexo.title()}"}}

    #Etapa para agrupar los documentos por el barrio y sumar la gente empadronada 
    etapa2={"$group":{"_id":"$barrio_nombre", f"{sexo.title()}":{"$sum":"$poblacion_empadronada"}}}

    #Etapa para ordenar por el campo del sexo segun el orden indicado
    etapa3={"$sort":{f"{sexo.title()}":orden}}

    #Etapa para limitar el numero de documentos a 5
    etapa4={"$limit":5}

    #Lista con las etapas del pipeline
    pipeline=[etapa1, etapa2, etapa3, etapa4]

    #Utilizamos la funcion aggregate para realizar las etapas y convertir a lista lo que devuelva
    muj_barrio=list(coleccion.aggregate(pipeline))

    #Lista para almacenar la cantidad de poblacion
    lista_cantidad=[]

    #lista para almacenar los nombres de los barrios
    lista_barrios=[]

    #Iteramos a traves de la lista
    for i in muj_barrio:

        #Mostramos por pantalla los documentos obtenidos de nuestra coleccion
        printer.pprint(i)

        #Agregamos la cantidad de poblacion a la lista
        lista_cantidad.append(i[f"{sexo.title()}"])

        #Agregamos el nombre del barrio a la lista
        lista_barrios.append(i["_id"])

    #Creamos un grafico de barras con ambas listas
    plt.bar(lista_barrios,lista_cantidad)

    #Mostramos el grafico
    plt.show()

"""
Llamada a la funcion para obtener el top 5 de barrios con mas o menos gente segun el sexo indicado
Es necesario pasarle el tipo de top (mas con -1 y menos con 1) y el sexo
"""
#top5_barrios_orden_sexo(-1, "mujer")

#Creamos una funcion para conocer la media de gente empadronada segun el rango de edad y un orden
def media_por_edad(orden):

    #Etapa para agrupar los documentos por el rango de edad y hacer la media de la poblacion
    etapa1={"$group":{"_id":"$rango_edad", "media":{"$avg":"$poblacion_empadronada"}}}

    #Etapa para ordenar por el campo de la media segun el orden indicado
    etapa2={"$sort":{"media":orden}}

    #Lista con las etapas del pipeline
    pipeline=[etapa1, etapa2]

    #Utilizamos la funcion aggregate para realizar las etapas y convertir a lista lo que devuelva
    media_gente=list(coleccion.aggregate(pipeline))

    #Lista para almacenar la media de poblacion
    lista_media=[]

    #lista para almacenar el rango
    lista_rangos=[]

    #Iteramos a traves de la lista
    for i in media_gente:

        #Mostramos por pantalla los documentos obtenidos de nuestra coleccion
        printer.pprint(i)

        #Agregamos la media de poblacion a la lista
        lista_media.append(i["media"])

        #Agregamos el nombre del barrio a la lista
        lista_rangos.append(i["_id"])

    #Creamos un grafico circular con ambas listas
    plt.pie(lista_media, labels=lista_rangos)

    #Mostramos el grafico
    plt.show()

"""
Llamada a la funcion para obtener la media de gente empadronada de cada rango de edad
Es necesario pasarle el orden de la media 
"""
#media_por_edad(-1)

#Creamos una funcion para saber la gente que hay de mas de 100 años empadronadas por el sexo
def mas_100_sexo():

    #Etapa para filtrar los documentos por el rango de edad
    etapa1={"$match":{"rango_edad":{"$regex":"100"}}}

    #Etapa para agrupar los documentos por el sexo y hacer la suma de la poblacion
    etapa2={"$group":{"_id":"$sexo", "suma":{"$sum":"$poblacion_empadronada"}}}

    #Etapa para ordenar por el campo de la suma de manera descendente
    etapa3={"$sort":{"suma":-1}}

    #Lista con las etapas del pipeline
    pipeline=[etapa1, etapa2, etapa3]

    #Utilizamos la funcion aggregate para realizar las etapas y convertir a lista lo que devuelva
    sexo_mas_100=list(coleccion.aggregate(pipeline))

    #Recogemos, del indice 0 de la lista y la clave "_id", el valor
    mas=sexo_mas_100[0]["_id"]

    #Recogemos, del indice 0 de la lista y la clave "suma", el valor
    valor_mas=sexo_mas_100[0]["suma"]

    #Recogemos, del indice 1 de la lista y la clave "_id", el valor
    menos=sexo_mas_100[1]["_id"]

    #Recogemos, del indice 1 de la lista y la clave "suma", el valor
    valor_menos=sexo_mas_100[1]["suma"]

    #Mostramos por pantalla el sexo y la cantidad de gente empadronada de mas de 100 años
    print(f"Para un rango de mas de 100 años, hay mas {mas} ({valor_mas}) que {menos} ({valor_menos})")

    #Creamos un array con los valores obtenidos con la funcion array()
    grafico=np.array([valor_mas, valor_menos])

    #Creamos un lista con los textos de cada valor
    etiquetas=[mas, menos]

    #Con el array creamos el grafico circular o de tarta con la funcion pie()
    plt.pie(grafico, labels=etiquetas)

    #Mostramos el grafico por pantalla con la funcion show()
    plt.show()

#Llamada a la funcion para obtener la gente que hay de mas de 100 años empadronadas por el sexo
#mas_100_sexo()

#Creamos una funcion para concatenar el barrio y el distrito
def concatenar_barrio_distrito():

    #Etapa para concatenar los campos de barrio y el distrito
    etapa1={"$project":{"_id":0,"concatenado":{"$concat":["El barrio de ", "$barrio_nombre", " pertenece al distrito de ", "$distrito_nombre"]}}}

    #Etapa para agrupar los documentos por el campo concatenado
    etapa2={"$group":{"_id":"$concatenado"}}

    #Lista con las etapas del pipeline
    pipeline=[etapa1, etapa2]

    #Utilizamos la funcion aggregate para realizar las etapas y convertir a lista lo que devuelva
    concatenacion=list(coleccion.aggregate(pipeline))

    #Iteramos a traves de la lista
    for i in concatenacion:

        #Mostramos por pantalla los documentos obtenidos de nuestra coleccion
        printer.pprint(i)

#Llamada a la funcion para obtener los barrios concatenados con su distrito
#concatenar_barrio_distrito()

#Creamos una funcion para obtener los empadronados del barrio de Las Aguilas
def regex_las_aguilas():

    #Etapa para filtrar los documentos por el barrio de Las Aguilas
    etapa1={"$match":{"barrio_nombre":{"$regex":"^La.*A[^c]"}}}

    #Etapa para agrupar los documentos por el barrio de Las Aguilas y sumar la poblacion empadronada
    etapa2={"$group":{"_id":"$barrio_nombre", "suma":{"$sum":"$poblacion_empadronada"}}}

    #Etapa para mostrar solo el nombre del barrio y la gente empadronada
    etapa3={"$project":{"barrio_nombre":1, "suma":1}}

    #Lista con las etapas del pipeline
    pipeline=[etapa1, etapa2, etapa3]

    #Utilizamos la funcion aggregate para realizar las etapas y convertir a lista lo que devuelva
    gente_aguilas=list(coleccion.aggregate(pipeline))

    #Recogemos, del indice 0 de la lista y la clave "_id", el valor
    barrio=gente_aguilas[0]["_id"]

    #Recogemos, del indice 0 de la lista y la clave "suma", el valor
    cantidad=gente_aguilas[0]["suma"]

    #Mostramos por pantalla la gente de Las Aguilas empadronada
    print(f"En el barrio de {barrio} hay {cantidad} personas empadronadas")

#Llamada a la funcion para obtener los empadronados del barrio de Las Aguilas
#regex_las_aguilas()

#Creamos una funcion para obtener el barrio numero n de un distrito
def barrio_n(distrito, n):

    #Pasamos a string el numero introducido
    posicion=str(n)

    #Etapa para filtrar los documentos por el distrito introducido
    etapa1={"$match":{"distrito_nombre":distrito.title()}}

    #Etapa para concatenar el codigo del distrito con la posicion del barrio
    etapa2={"$project":{"_id":0,"cod_barrio":{"$concat":["$distrito_codigo",posicion]}}}

    #Etapa para agrupar el codigo del barrio
    etapa3={"$group":{"_id":"$cod_barrio"}}

    #Lista con las etapas del pipeline
    pipeline=[etapa1, etapa2, etapa3]

    #Utilizamos la funcion aggregate para realizar las etapas y convertir a lista lo que devuelva
    codig_barr=list(coleccion.aggregate(pipeline))

    #Recogemos, del indice 0 de la lista y la clave "_id", el valor del codigo del barrio
    barrio_buscar=codig_barr[0]["_id"]

    #Utilizamos la funcion find_one para obtener un registro con ese codigo del barrio
    registros=coleccion.find_one({"barrio_codigo":barrio_buscar}, {"barrio_nombre":1, "barrio_codigo":1, "_id":0})

    #Recogemos, de la clave "barrio_nombre", el valor 
    nom=registros["barrio_nombre"]

    #Mostramos por pantalla el barrio que ocupa la posicion introducida
    print(f"El barrio con la posicion {n} dentro del distrito {distrito.title()} es {nom}")

"""
Llamada a la funcion para obtener el barrio a trave de su posicion en el distrito
Es necesario pasarle la posicion del barrio dentro del distrito
"""
#barrio_n("chamartín",2)


#Creamos una funcion para saber cuantos barrios tiene cada distrito
def barrios_distritos():

    #Etapa para agrupar los documentos por el distrito y el barrio
    etapa1={"$group":{"_id":{"distrito":"$distrito_nombre","barrio":"$barrio_nombre"}}}

    #Etapa para agrupar por el distrito y sumar el numero de documentos
    etapa2={"$group":{"_id":"$_id.distrito", "suma":{"$sum":1}}}

    #Etapa para ordenar de manera alfabetica los distritos
    etapa3={"$sort":{"_id":1}}

    #Lista con las etapas del pipeline
    pipeline=[etapa1, etapa2,etapa3]

    #Utilizamos la funcion aggregate para realizar las etapas y convertir a lista lo que devuelva
    barr_distri=list(coleccion.aggregate(pipeline))

    #Lista para almacenar la cantidad de barrios del distrito
    lista_cantidad=[]

    #lista para almacenar los nombres de los distritos
    lista_distritos=[]

    #Iteramos a traves de la lista
    for i in barr_distri:

        #Mostramos por pantalla los documentos obtenidos de nuestra coleccion
        print(i)

        #Agregamos la cantidad de barrios a la lista
        lista_cantidad.append(i["suma"])

        #Agregamos el nombre del distrito a la lista
        lista_distritos.append(i["_id"])

    #Creamos un grafico de barras horizontal con ambas listas
    plt.barh(lista_distritos[::-1],lista_cantidad[::-1])

    #Mostramos el grafico
    plt.show()

#Llamada a la funcion para obtener el numero de barrios de cada distrito
#barrios_distritos()





    

        