from api.RMapi import mongo
from api.neo import neo
from api.cass import cass
from api.etl import etl

def main():
    mongo()
    etl()
    neo()
    cass()

if __name__ == "__main__":
    main()
    