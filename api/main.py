from RMapi import mongo
from neo import neo
from cass import cass

def main():
    mongo()
    neo()
    cass()

if __name__ == "__main__":
    main()
    