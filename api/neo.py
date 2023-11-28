from neo4j import GraphDatabase




def neo():
        # Neo4j connection parameters
    uri = "bolt://localhost:7687"
    username = "neo4j"
    password = "12345678"

    # funcion que carga csv a neo4j
    def load_csv(url, node_label):
        query = (
            f"LOAD CSV WITH HEADERS FROM '{url}' AS row "
            f"CREATE (n:{node_label}) SET n = row"
        )
        return query
    

    # carga csv a neo4j
    csv_files =  ['https://raw.githubusercontent.com/maudiaz221/NoSQLProject/main/data/locations.csv','https://raw.githubusercontent.com/maudiaz221/NoSQLProject/main/data/episodes.csv','https://raw.githubusercontent.com/maudiaz221/NoSQLProject/main/data/characters.csv']

    node_labels = ["location", "episode", "character"]
    
    query1 = '''
        MATCH (c:character),(e:episode)
        WHERE c.episode_id = e.episode_id
        CREATE (c)-[r:APPEARED_IN]->(e)
    '''
    query2 = '''
        MATCH (c:character),(l:location)
        WHERE c.character_id = l.character_id
        CREATE (c)-[r:LOCATED_IN]->(l)
    '''
    query3 = '''
    MATCH (c:character)-[:LOCATED_IN]->(l:location)
    MERGE (e:episode)-[:APPEARED_IN]->(c)-[:LOCATED_IN]->(l)
    '''

    #crea sesion y corre los queries y guarda los datos en neo4j
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        for csv_file, node_label in zip(csv_files, node_labels):
            with driver.session() as session:
                query = load_csv(csv_file, node_label)
                session.run(query)
                try:
                    session.run(query1)
                    session.run(query2)
                    session.run(query3)
                except:
                    print("Error al crear las relaciones")
            with driver.session() as session:
                try:
                    query_1 = """
                            MATCH (character:character)
                            WHERE character.species = "Human"
                            RETURN character
                            """

                    query_2 = """
                            MATCH (character:character)-[:APPEARED_IN]->(episode:episode)
                            WHERE episode.name IN ["Meeseeks and Destroy", "Morty's Mind Blowers"]
                            RETURN character
                            """

                    query_3 = """
                            MATCH (e:episode)-[:APPEARED_IN]->(c:character)-[:LOCATED_IN]->(l:location)
                            RETURN e, c, l
                            LIMIT 10
                            """
                    # Ejecuta queries e imprime resultados
                    print("Query 1 Result:")
                    
                    result_1 = session.run(query_1)
                    for record in result_1:
                        print(record)

                    print("\nQuery 2 Result:")
                    result_2 = session.run(query_2)
                    for record in result_2:
                        print(record)

                    print("\nQuery 3 Result:")
                    result_3 = session.run(query_3)
                    for record in result_3:
                        print(record)
                except:
                    print("")
    
    print("Datos insertados")
                    
                
   
    
    
    
                
if __name__ == "__main__":
    neo()