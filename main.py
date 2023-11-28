from api.RMapi import mongo
from api.neo import neo
from api.cass import cass

def main():
    mongo()
    neo()
    cass()

if __name__ == "__main__":
    main()
    