from cassandra.cluster import Cluster
import pandas as pd

def cass():

    KEYSPACE = "test"

    cluster = Cluster()
    session = cluster.connect()


    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} 
        """ % KEYSPACE)


    session.set_keyspace(KEYSPACE)

    session.execute("""
        DROP TABLE IF EXISTS test.characters;
        """)


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
        
    # Create the episode table
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

    # Create the locations table
    session.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            location_id int,
            name text,
            type text,
            dimension text,
            url text,
            created date,
            character_id int,
            PRIMARY KEY(location_id)
        );
    """)

    # Read data from locations.csv
    df_locations = pd.read_csv('data/locations.csv')

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





