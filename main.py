from fastapi import FastAPI
import pandas as pd
import pyarrow.parquet as pq

app = FastAPI()



@app.get("/developer/{desarrollador}")
def developer(desarrollador:str):
    df = pd.read_json('Datasets/Steam_Games_Limpio.json.gz', compression='gzip')
    df = df[['price','developer','release_date']]
    desarrollador = desarrollador.title() #Ponemos como esta en los datasets

    #Verificamos si existe el developer pedido
    if (desarrollador in df['developer'].unique()):
        df = df[df['developer'].apply(lambda x: desarrollador in x)]
        free = df[df['price'] == 0.00]
    else:
        return {'Error':'No existe el desarrollador'}

    #Creamos una lista ordenada de los años que lanzaron un juego.
    anios = sorted(list(df['release_date'].unique()))

    # Creamos un dataframe con la lista de los años y dejamos ya las columnas pedidas.
    dataRespuesta = pd.DataFrame(columns=['Año', 'Cantidad de Items', 'Contenido Free'])
    dataRespuesta['Año'] = anios

    #Modificamos los dataframe para optimizar el ciclo que le sigue.
    df = df.groupby('release_date').groups
    free= free.groupby('release_date').groups

    # Agregamos los valores al dataframe dataRespuesta.
    for anio in anios:
        total = df[anio].shape[0]   #Sacamos el total de items que tiene el desarrollador en x anio
        dataRespuesta.loc[dataRespuesta['Año'] == anio, 'Cantidad de Items'] = total
        # Utilizo un try except para que los que no tienen contenido free al tener None tira error que seria que no tiene ninguno free.
        try:
            cant_free = free[anio].shape[0]
            dataRespuesta.loc[dataRespuesta['Año'] == anio, 'Contenido Free'] = f'{round((cant_free/total)*100,2)}%'
        except KeyError:
            dataRespuesta.loc[dataRespuesta['Año'] == anio, 'Contenido Free'] = '0%'

    respuesta = []
    for i in dataRespuesta.values:
        respuesta.append({'Año':i[0],'Cantidad de Items':i[1], 'Contenido Free': i[2]})

    return respuesta




@app.get("/userdata/{usuario}")
def userdata(usuario:str):
    user_items = pq.read_table('Datasets/items.parquet').to_pandas()

    #Verificamos si existe el usuario
    if (usuario in user_items['user_id'].unique()):
        user_items = user_items[user_items['user_id'] == usuario] #Filtramos user_items por el usuario pedido
    else:
        return {'Error':'No existe el usuario.'}
    
    #Cargamos los datasets
    juegos = pd.read_json('Datasets/Steam_Games_Limpio.json.gz', compression='gzip')
    juegos = juegos[['id','price']]
    recomendaciones = pd.read_json('Datasets/User_Reviews_Limpio.json.gz', compression='gzip')
    recomendaciones = recomendaciones[['user_id','recommend']]

    # Gurdamos la cantidad de items del usuario
    cant_items = user_items['items_count'].iloc[0] 

    #Vemos las recomendaciones que tiene el usuario pedido
    recomendaciones = recomendaciones[recomendaciones['user_id'] == usuario]['recommend']    
    cant_total_recomendaciones = recomendaciones.shape[0] #Cantidad total de recomendaciones
    recomendaciones_postitvas = recomendaciones.value_counts()[True] # Nos quedamos con la cantidad de que son positivas
    porcentaje_de_recomendaciones = f'{round((recomendaciones_postitvas/cant_total_recomendaciones)*100,2)}%' # Porcentaje total de las recomendaciones en formato str.

    # Hacemos verificamos los items que tiene para poder calcular el monto gastado con la informacion que tenemos.
    user_items = user_items.explode('items')
    user_items =  user_items['items'].apply(pd.Series) # Creamos un dataset con los items
    user_items['item_id'] = user_items['item_id'].astype(int)
    juegos = juegos.merge(user_items, left_on='id', right_on='item_id') #Combinamos los 2 datasets para poder tener la infromacion toda junta tiene que ser de tipo innner ya que los que no se encuentren en el dataset de steam se vayan y que den solo la interseccion de ambos datasets. 
    gasto_total = f"{round(juegos['price'].sum(),2)} USD" # Sumamos todos los precios porque al estar filtrados con solo los que tiene el usuario nos daria el monto total gastado. El round esta por que en algunas pruebas me daba mas de 2 decimales aunque esten corregidos en el ETL.
    return {'Usuario': f'{usuario}', 'Dinero gastado': gasto_total, "% de recomendación": f'{porcentaje_de_recomendaciones}', "cantidad de items": f'{cant_items}'}




@app.get("/UserForGenre/{genero}")
def UserForGenre(genero):
    #Leemos el dataframe
    df_horasXgenero = pd.read_json('Datasets//userForGenre.json.gz', compression='gzip')

    #Nos fijamos si el genero pedido se encuentra en la lista de generos
    if genero in df_horasXgenero['genero'].unique():
        df_horasXgenero = df_horasXgenero[df_horasXgenero['genero'] == genero]
        usuario = df_horasXgenero['usuario'].iloc[0] #Agarramos el usuario
        horasJugadas = df_horasXgenero['Horas jugadas'].iloc[0] #Agarramos la cantidad de horas por año
        return {f'Usuario con más horas jugadas para Género {genero}': usuario, "Horas jugadas": horasJugadas}
    else:
        return {'Mensaje':'No se encuentran horas registradas para este genero.'}




@app.get("/best_developer_year/{anio}")
def best_developer_year(anio:int):
    developers = pd.read_json('Datasets/Steam_Games_Limpio.json.gz', compression='gzip')
    developers = developers[['release_date','id','developer']]

    #Verificamos si existe el año pedido.
    if anio in developers['release_date'].unique():
        developers = developers[developers['release_date'] == anio] #Nos quedamos con el año que necesitamos
    else:
        return {'Error':'No hay ningun lanzamiento ese año'}

    user_reviews = pd.read_json('Datasets/User_Reviews_Limpio.json.gz', compression='gzip')
    user_reviews = user_reviews[['item_id','recommend','sentiment_analysis']]
    user_reviews = user_reviews.merge(developers, left_on='item_id', right_on='id')[['developer','sentiment_analysis','recommend']] # Unimos los datasets para que el manejo sea mas facil

    # Reemplazo el tipo de dato a str para poder utilizar el replace y vuevlo a ponerlo a int para posteriormente sumarlos y obtener la cantidad de positivos ya que el unico q me interesa que sume es el mismo.
    user_reviews['sentiment_analysis'] = user_reviews['sentiment_analysis'].astype(str) 
    user_reviews['sentiment_analysis'].replace('1','0', inplace=True)
    user_reviews['sentiment_analysis'].replace('2','1', inplace=True)
    user_reviews['sentiment_analysis'] = user_reviews['sentiment_analysis'].astype(int)

    # El valor booleano de recomend lo remplazo a int con el mismo fin para poder sumarlos como en sentiment_analysis
    user_reviews['recommend']=user_reviews['recommend'].astype(int) 
    
    # Junto todo por developper para despues crear una columna nueva con la suma de la linea por linea.
    user_reviews = user_reviews.groupby('developer').sum()
    user_reviews['masRecomendados'] =  user_reviews.sum(axis=1)

    # Creo el top 3 para mas comodidad.
    top3 = user_reviews['masRecomendados'].sort_values(ascending=False).head(3).index
    
    return {'Puesto 1': top3[0], 'Puesto 2': top3[1], 'Puesto 3': top3[2]}




@app.get("/developer_reviews_analysis/{desarrollador}")
def developer_reviews_analysis(desarrollador:str):
    developers = pd.read_json('Datasets/Steam_Games_Limpio.json.gz', compression='gzip')
    developers = developers[['id','developer']]
    desarrollador = desarrollador.title()

    #Verificamos si existe el desarrolador
    if (desarrollador in developers['developer'].unique()): 
        developers = developers[developers['developer'].apply(lambda x: desarrollador in x)] # Filtramos por dessarrolador
    else:
        return {'Error':'No existe el desarrollador'}

    user_reviews = pd.read_json('Datasets/User_Reviews_Limpio.json.gz', compression='gzip')
    user_reviews = user_reviews[['item_id','sentiment_analysis']]

    #Unimos los datasets, con inner asi los que no se encuentran en ambos no figuran.
    developers = developers.merge(user_reviews, left_on='id', right_on='item_id')

    # Hacemos un recuento de entiment_analysis para posteriormente guardar los positivos y negativos.
    valoresDeVotacion = developers['sentiment_analysis'].value_counts()

    positivos = valoresDeVotacion[2]
    negativos = valoresDeVotacion[0]

    return {desarrollador: [f'Negative = {negativos}', f'Positive = {positivos}']}

