import os
import requests
from pymongo import MongoClient

import certifi
ca = certifi.where()

from pymongo.database import Database

from dotenv import load_dotenv
load_dotenv()

DB_URI = os.environ.get('MONGO_DATABASE')
WAVE_API = os.environ.get('WAVE_API')


class ReportProcessor:

    def process_spots(self, db: Database, spotIds: list):
        """
        Processes all the spots
        """

        for spotId in spotIds:
            wave_data = self.fetch_data(WAVE_API + spotId)
            return
      
    def fetch_data(self, url: str):
        """
        Get's the specified data
        """
        
        r = requests.get(url)
        json = r.json()
        data = json['data']
        




    def remove_old_data(self):

        print('removing data')

def main():
    
    # Connect to db
    client = MongoClient(DB_URI, tlsCAFile=ca)
    db = client['surf']

    # Grab all the spots
    spots = db['Spots'].find({}, {
        'spotId': 1
    })
    
    # Get all the spotIds in a list
    spotIds = []
    for spot in spots:
        spotIds.append(spot['spotId'])

    processor = ReportProcessor()
    processor.process_spots(db, spotIds)


if __name__ == "__main__":
    main()