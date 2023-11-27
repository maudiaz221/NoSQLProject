import requests
from RMapi import fetch_RM_data
import pandas as pd

urls = ['https://rickandmortyapi.com/api/location','https://rickandmortyapi.com/api/character','https://rickandmortyapi.com/api/episode']

def main():
    csvs()
    print("CSVs creados")
    

def csvs():

    for url in urls:
        data = fetch_RM_data(url)
        #los datos obtenidos los convertimos en un dataframe
        df = pd.DataFrame(data, index=None)
        #pasamos el dataframe a csv
        if url == urls[0]:
            df.to_csv('locations.csv', index=False)
        elif url == urls[1]:
            df.to_csv('characters.csv', index=False)
        elif url == urls[2]:
            df.to_csv('episodes.csv', index=False)

def insertNeo4j():
    pass
    
    
if __name__ == "__main__":
    main()