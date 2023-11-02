#Importamos dependencias para descargar los datos de la API de Rick and Morty
import requests
import pymongo

#creamos instancia del mongo
cliente = pymongo.MongoClient("mongodb://localhost:27017/")

#creamos la base de datos
db = cliente["RM"]

#creamos la coleccion
coleccion = db['characters']

def fetch_RM_data():
    
    url = 'https://rickandmortyapi.com/api/character'
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




if __name__ == "__main__":
    data = fetch_RM_data()
    insertData(data,coleccion)
    
 


 



    

    


    
        

