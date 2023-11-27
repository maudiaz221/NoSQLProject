from cassandra.cluster import Cluster

KEYSPACE = "test_cassandra"

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()


session.execute("""
    CREATE KEYSPACE IF NOT EXISTS %s 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} 
    """ % KEYSPACE)


session.set_keyspace(KEYSPACE)


session.execute("""
    CREATE TABLE IF NOT EXISTS test_NYC_taxi (
        pickup timestamp, 
        dropoff timestamp, 
        distance decimal, 
        fare decimal, 
        p_long decimal, 
        p_lat decimal, 
        d_long decimal, 
        d_lat decimal, 
        PRIMARY KEY(pickup, dropoff, distance));
    """)



prepared = session.prepare("""
        INSERT INTO test_nyc_taxi (pickup, dropoff, distance, fare, p_long, p_lat, d_long, d_lat)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
