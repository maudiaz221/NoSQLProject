#Importamos dependencias para descargar los datos de la API de Rick and Morty
import requests
import pymongo
import pandas as pd


def mongo():
    #creamos instancia del mongo
    cliente = pymongo.MongoClient("mongodb://mongo:27017/")

    #creamos la base de datos
    db = cliente["RM"]

    #creamos la coleccion
    colecciones = ['locations','characters','episodes']


    def fetch_RM_data(url):
        

        characters = []
        r = requests.get(url)
        data = r.json()
        amount = data['info']['count']
        #recorremos todos los personajes
        for i in range(1,amount+1):
            r = requests.get(url + '/' + str(i))
        #Revisamos que el status code haya sido efectivo
            if r.status_code == 200:
                try:
                    #pasamos los datos a un json
                    data = r.json()
                    #agregamos cada caracter a una lista
                    characters.append(data) 
                except ValueError as e:
                    print("Error al convertir a JSON" + str(e))
            else:
                print("error al descargar los datos")
        
        return characters

    def insertData(data,collection):
        #insertamos los datos en la coleccion
        for character in data:
            collection.insert_one(character)
        print("Datos insertados")
    
    #agarramos todas las colecciones de la base de datos
    collections = db.list_collection_names()
    
    for collection in collections:
        db[collection].drop()
    
    print('colleciones borradas')
        
    urls = ['https://rickandmortyapi.com/api/location','https://rickandmortyapi.com/api/character','https://rickandmortyapi.com/api/episode']
    #lo guarda en la coleccion dependiendo del nombre
    for url in urls:
        data = fetch_RM_data(url)
        if url == urls[0]:
            locations = db[colecciones[0]]
            insertData(data,locations)
        elif url == urls[1]:
            collection = db[colecciones[1]]
            insertData(data,collection)
        elif url == urls[2]:
            episodes = db[colecciones[2]]
            insertData(data,episodes)
        else:
            print("error al insertar los datos")
    
    
    for collection in collections:
        #agarrar documentos de la coleccion
        documents = db[collection].find()
        
        #conviertes en lista para un dataframe
        df = pd.DataFrame(list(documents))
        
        df.to_csv(f"api/pretransformed_data/{collection}.csv", index=False,mode='w')
    
    print("Datos hechos")
        
    
    # try:
    #     # Query 1
    #     query_1_result = collection.aggregate([
    #         {"$match": {"species": "Human", "gender": "Male"}},
    #         {"$project": {"_id": 0, "species": 1, "gender": 1, "name": 1}}
    #     ])

    #     print("Query 1 Result:")
    #     for document in query_1_result:
    #         print(document)

    #     # Query 2
    #     query_2_result = collection.aggregate([
    #         {"$unwind": "$origin"},
    #         {"$project": {"_id": 0, "origin.name": 1, "status": 1}},
    #         {"$match": {"status": "Alive"}},
    #         {"$group": {"_id": "$origin.name", "conteo": {"$sum": 1}}}
    #     ])

    #     print("\nQuery 2 Result:")
    #     for document in query_2_result:
    #         print(document)

    #     # Query 3
    #     query_3_result = collection.aggregate([
    #         {"$unwind": "$location"},
    #         {"$project": {"location.name": 1, "id": 1, "species": 1}},
    #         {"$group": {"_id": "$species", "locationArray": {"$push": "$location.name"}}},
    #         {"$project": {"name": "$_id", "_id": 1, "locationArray": 1}},
    #         {"$addFields": {"totalHabitantes": {"$sum": 1}}}
    #     ])

    #     print("\nQuery 3 Result:")
    #     for document in query_3_result:
    #         print(document)
    # except:
    #     print("")



if __name__ == "__main__":
    mongo()
    
 


 



    

    


    
        

