from neo4j import GraphDatabase
import pandas as pd



def neo():
        # Neo4j connection parameters
    uri = "bolt://neo4j:7687" #uri='bolt://neo4j:7687'
    username = "neo4j"
    password = "12345678"

    # Archivos CSV y etiquetas de nodo
    archivos_csv = ['api/data/locations.csv', 'api/data/episodes.csv', 'api/data/characters.csv']
    etiquetas_nodo = ["Location", "Episode", "Character"]

    consultas_relacion = [
        '''
        MATCH (c:Character),(e:Episode)
        WHERE c.character_id = e.character_id
        CREATE (c)-[r:APPEARED_IN]->(e)
        ''',
        '''
        MATCH (c:Character),(l:Location)
        WHERE c.character_id = l.character_id
        CREATE (c)-[r:LOCATED_IN]->(l)
        '''
    ]

    # Crear una sesión, ejecutar consultas y almacenar datos en Neo4j
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        with driver.session() as session:
            # Eliminar todos los nodos antes de cargar nuevos datos
            session.run('''MATCH (n) DETACH DELETE n; ''')

            # Iterar a través de los archivos CSV y etiquetas de nodo
            for archivo_csv, etiqueta_nodo in zip(archivos_csv, etiquetas_nodo):
                df = pd.read_csv(archivo_csv)

                # Iterar a través de las filas del DataFrame y crear nodos
                for indice, fila in df.iterrows():
                    propiedades = fila.to_dict()
                    # Crear nodo con propiedades
                    session.run(f"CREATE (n:{etiqueta_nodo}) SET n = $propiedades", propiedades=propiedades)

                # Iterar a través de las consultas de relación y ejecutarlas
                for consulta_relacion in consultas_relacion:
                    session.run(consulta_relacion)
            # with driver.session() as session:
            #     try:
            #         query_1 = """
            #                 MATCH (character:character)
            #                 WHERE character.species = "Human"
            #                 RETURN character
            #                 """

            #         query_2 = """
            #                 MATCH (character:character)-[:APPEARED_IN]->(episode:episode)
            #                 WHERE episode.name IN ["Meeseeks and Destroy", "Morty's Mind Blowers"]
            #                 RETURN character
            #                 """

            #         query_3 = """
            #                 MATCH (e:episode)-[:APPEARED_IN]->(c:character)-[:LOCATED_IN]->(l:location)
            #                 WITH e, COUNT(DISTINCT l) AS locationCount
            #                 RETURN AVG(locationCount) AS avgLocationsPerEpisode

            #                 """
            #         # Ejecuta queries e imprime resultados
            #         print("Query 1 Result:")
                    
            #         result_1 = session.run(query_1)
            #         for record in result_1:
            #             print(record)

            #         print("\nQuery 2 Result:")
            #         result_2 = session.run(query_2)
            #         for record in result_2:
            #             print(record)

            #         print("\nQuery 3 Result:")
            #         result_3 = session.run(query_3)
            #         for record in result_3:
            #             print(record)
            #     except:
            #         print("")
    
    print("Datos insertados")
                    
                
   
    
    
    
                
if __name__ == "__main__":
    neo()