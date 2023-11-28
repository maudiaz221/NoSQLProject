from cassandra.cluster import Cluster
import pandas as pd

'''
Lo que hace el codigo de aqui es crear una sesion en cassandra y crear las tablas
inserta los datos en las tablas a traves de pandas y luego ejecuta las consultas

'''

def cass():
    
    KEYSPACE = "test"
    #crea la sesion
    cluster = Cluster()
    session = cluster.connect()

    #crea el keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} 
        """ % KEYSPACE)


    session.set_keyspace(KEYSPACE)

    session.execute("""
        DROP TABLE IF EXISTS test.characters;
        """)

    #crea tabla 1
    session.execute("""
        CREATE TABLE IF NOT EXISTS characters (
            character_id int, 
            name text,
            status text,
            species text,
            type text,
            gender text,
            origin text,
            location text,
            image text,
            url text,
            created date,
            episode_id int,
            PRIMARY KEY(character_id));
        """)

    df = pd.read_csv('data/characters.csv')

    for index, row in df.iterrows():
        row = row.fillna('') 
        session.execute("""
            INSERT INTO test.characters (
                character_id, name, status, species, type, gender, origin, location, image, url, created, episode_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            row['character_id'], row['name'], row['status'], row['species'], row['type'],
            row['gender'], row['origin'], row['location'], row['image'], row['url'],
            pd.to_datetime(row['created']).strftime('%Y-%m-%d'), row['episode_id']
        ))
        
    # crea tabla 2
    session.execute("""
        CREATE TABLE IF NOT EXISTS episodes (
            episode_id int,
            name text,
            air_date date,
            episode text,
            character_id int,
            url text,
            created date,
            PRIMARY KEY(episode_id)
        );
    """)

    # Read data from episodes.csv
    df_episodes = pd.read_csv('data/episodes.csv')

    # Insert data into the episode table
    for index, row in df_episodes.iterrows():
        row = row.fillna('') 
        session.execute("""
            INSERT INTO test.episodes (
                episode_id, name, air_date, episode, character_id, url, created
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (
            row['episode_id'], row['name'], pd.to_datetime(row['air_date']).strftime('%Y-%m-%d'),
            row['episode'], row['character_id'], row['url'], pd.to_datetime(row['created']).strftime('%Y-%m-%d')
        ))

    # crea tabla 3
    session.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            location_id int,
            name text,
            type text,
            dimension text,
            url text,
            created date,
            character_id double,
            PRIMARY KEY(location_id)
        );
    """)

    # Read data from locations.csv
    df_locations = pd.read_csv('data/locations.csv')
    df_locations['character_id'] = df_locations['character_id'].fillna(0).astype(int)

   # ...
    # Insert data into the locations table
    for index, row in df_locations.iterrows():
        row = row.fillna('') 
        session.execute("""
            INSERT INTO test.locations (
                location_id, name, type, dimension, url, created, character_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (
            row['location_id'], row['name'], row['type'], row['dimension'], row['url'],
            pd.to_datetime(row['created']).strftime('%Y-%m-%d'), row['character_id']
        ))

    
    try:
                # Las queries
        query_1 = "SELECT name FROM episodes;"
        query_2 = "SELECT * FROM characters WHERE character_id=671;";
        query_3 = "SELECT * FROM locations;"

        # Funcion que ejecuta query
        def run_query(query):
            result = session.execute(query)
            return result

        # ejecuta
        print("Query 1 Result:")
        result_1 = run_query(query_1)
        for row in result_1:
            print(row)

        print("\nQuery 2 Result:")
        result_2 = run_query(query_2)
        for row in result_2:
            print(row)

        print("\nQuery 3 Result:")
        result_3 = run_query(query_3)
        for row in result_3:
            print(row)
        
    except:
        print("")


cass()




