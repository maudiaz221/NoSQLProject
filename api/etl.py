import pandas as pd
import ast

def etl():

    #primera transformacion a ubicacion
    df = pd.read_csv('api/pretransformed_data/locations.csv')

    # remplazar valores nulos con listaa vacia
    df['residents'] = df['residents'].apply(lambda x: ast.literal_eval(x) if x is not None else [])
    # explotar la columna residents
    df = df.explode('residents').reset_index(drop=True)
    df['character_id'] = df['residents'].str.extract(r'(\d+)')
    df.drop(columns=['residents','_id'], inplace=True)
    df.rename(columns={'id': 'location_id'}, inplace=True)
    df.to_csv('api/data/locations.csv', index=False, mode='w')


    #segunda transformacion a personajes
    df1 = pd.read_csv('api/pretransformed_data/characters.csv')

    # Remplazar valores nulos con una lista vacia
    df1['episode'] = df1['episode'].apply(lambda x: ast.literal_eval(x) if x is not None else [])
    # Explode the la columna de episodios
    df1 = df1.explode('episode').reset_index(drop=True)
    df1['episode_id'] = df1['episode'].str.extract(r'(\d+)')
    df1.drop(columns=['episode','_id'], inplace=True)
    pattern = r'''['"]name['"]: ['"]([^'"]*)['"]'''
    df1['origin'] = df1['origin'].str.extract(pattern)
    df1['location'] = df1['location'].str.extract(pattern)
    df1.rename(columns={'id': 'character_id'}, inplace=True)
    df1.to_csv('api/data/characters.csv', index=False, mode='w')


    #tercera transformacion a episodios
    df2 = pd.read_csv('api/pretransformed_data/episodes.csv')
    df2 = df2.rename(columns={'id': 'episode_id','characters': 'character_id'})
    # Remplazar valores nulos con una lista vacia
    df2['character_id'] = df2['character_id'].apply(lambda x: ast.literal_eval(x) if x is not None else [])
    # Explotar la columna de personajes
    df2 = df2.explode('character_id').reset_index(drop=True)
    df2['character_id'] = df2['character_id'].str.extract(r'(\d+)')
    df2.drop(columns=['_id'], inplace=True)
    df2.to_csv('api/data/episodes.csv', index=False, mode='w')

if __name__ == "__main__":
    etl()