import csv
import pymongo
from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient("0.0.0.0:2717")
db = client["test"]
# collections = db["heroinfo"]

def add_hero_info_to_db(hero_values: list):

    with open("dfkHeroData.csv", 'r') as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        rows = list()
        for row in csvreader:
            rows.append(row)
            hero_post = {
                "id": row[0],
                "username": row[1],
                "wallet_address": row[2],
                "mainclass": row[3],
                "level": row[4]
            }
            db.user.insert_one(hero_post)
        print(header)
        print(rows)

if __name__ == "__main__":
    add_hero_info_to_db([1,2,3,4,5,6,7])