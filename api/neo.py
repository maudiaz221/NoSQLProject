from neo4j import GraphDatabase




def main():
        # Neo4j connection parameters
    uri = "bolt://localhost:7687"
    username = "neo4j"
    password = "12345678"

    # Function to load CSV data into Neo4j
    def load_csv(url, node_label):
        query = (
            f"LOAD CSV WITH HEADERS FROM '{url}' AS row "
            f"CREATE (n:{node_label}) SET n = row"
        )
        return query
    

    # Load CSV files into Neo4j
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

    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        for csv_file, node_label in zip(csv_files, node_labels):
            with driver.session() as session:
                query = load_csv(csv_file, node_label)
                session.run(query)
                try:
                    session.run(query1)
                    session.run(query2)
                except:
                    print("Error al crear las relaciones")
    
    print("Datos insertados")
                    
                
   
    
    
    
                
if __name__ == "__main__":
    main()