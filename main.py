import os
import requests
from pymongo import MongoClient
from pprint import pprint
from datetime import datetime

import certifi
ca = certifi.where()

from pymongo.database import Database

from dotenv import load_dotenv
load_dotenv()

DB_URI = os.environ.get('MONGO_DATABASE')
WAVE_API = os.environ.get('WAVE_API')
WIND_API = os.environ.get('WIND_API')
WEATHER_API = os.environ.get('WEATHER_API')


class ReportProcessor:

    def process_spots(self, db: Database, spot_ids: list):
        """
        Processes all the spots
        """
        print(f'Processing {len(spot_ids)} spots')

        errors = []

        for spot_id in spot_ids:
            print(f'Processing spot_id: {spot_id}')

            # wave data
            wave_data = self.fetch_data(WAVE_API + spot_id)
            processed_wave_data = self.process_wave_data(wave_data)
            if processed_wave_data is None:
                print(f'No wave data returned for spot ({spot_id})')
                errors.append({
                    'error': "no-data-found",
                    'type': 'wave',
                    'message': 'No wave data was returned from the API',
                    'spot_id': spot_id
                })
                continue

            # wind data
            wind_data = self.fetch_data(WIND_API + spot_id)
            processed_wind_data = self.process_wind_data(wind_data)
            if processed_wind_data is None:
                print(f'No wind data returned for spot ({spot_id})')
                errors.append({
                    'error': "no-data-found",
                    'type': 'wind',
                    'message': 'No wind data was returned from the API',
                    'spot_id': spot_id
                })
                continue

            # weather data
            weather_data = self.fetch_data(WEATHER_API + spot_id)
            processed_weather_data = self.process_weather_data(weather_data)
            if processed_weather_data is None:
                print(f'No weather data returned for spot ({spot_id})')
                errors.append({
                    'error': "no-data-found",
                    'type': 'weather',
                    'message': 'No weather data was returned from the API',
                    'spot_id': spot_id
                })
                continue

            # merge all the data together
            report_records = self.merge_data(processed_wave_data, processed_wind_data, processed_weather_data, spot_id)
            if report_records is None:
                print(f'Data numbers do not add up: {len(processed_wave_data)}, {len(processed_wind_data)}, {len(processed_weather_data)}')
                errors.append({
                    'error': "incorrect-amount-of-data",
                    'message': 'The number of data from each api fetch was not equal.',
                    'spot_id': spot_id
                })
                continue

            # insert the reports    
            self.insert_wave_reports(db, report_records, spot_id)
        
        return errors


      
    def fetch_data(self, url: str) -> object:
        """
        Fetches data from the specified api.

        :url: The api endpoint
        """
        
        r = requests.get(url)
        json = r.json()
        return json['data']
        

    def process_wave_data(self, data: object) -> list:
        """
        Processes the wave data.

        :data: The wave report json object
        :return: list of wave data objects
        """

        hourly_wave_reports = data.get('wave', None)

        if hourly_wave_reports is None:
            return None
        
        processed_hourly_wave_reports = []
        for report in hourly_wave_reports:
            processed_hourly_wave_report = {
                'timestamp': datetime.fromtimestamp(int(report['timestamp'])),
                'utcOffset': report['utcOffset'],
                'waveHeightMin': report['surf']['min'],
                'waveHeightMax': report['surf']['max'],
                'humanRelation': report['surf']['humanRelation'],
                'swells': report['swells']
            }
            processed_hourly_wave_reports.append(processed_hourly_wave_report)

        return processed_hourly_wave_reports
    
    def process_wind_data(self, data: object) -> list:
        """
        Processes the wind data.

        :data: The wind report json object
        :return: List of wind data objects
        """

        hourly_wind_reports = data.get('wind', None)

        if hourly_wind_reports is None:
            return None
        
        processed_hourly_wind_reports = []
        for report in hourly_wind_reports:
            processed_hourly_wind_report = {
                'timestamp': datetime.fromtimestamp(int(report['timestamp'])),
                'windSpeed': report['speed'],
                'windDirection': report['direction'],
                'directionType': report['directionType'],
                'windGust': report['gust'],
            }
            processed_hourly_wind_reports.append(processed_hourly_wind_report)

        return processed_hourly_wind_reports
    
    def process_weather_data(self, data: object) -> list:
        """
        Processes the weather data.

        :data: The weather report json object
        :return: List of weather data objects
        """

        hourly_weather_reports = data.get('weather', None)

        if hourly_weather_reports is None:
            return None
        
        processed_hourly_weather_reports = []
        for report in hourly_weather_reports:
            processed_hourly_weather_report = {
                'timestamp': datetime.fromtimestamp(int(report['timestamp'])),
                'airTemperature': report['temperature'],
            }
            processed_hourly_weather_reports.append(processed_hourly_weather_report)

        return processed_hourly_weather_reports

    def merge_data(self, wave_data: list, wind_data: list, weather_data: list, spot_id: str) -> list:
        """
        Merge all three data lists into one list of surf report objects.

        :wave_data: list of wave data objects
        :wind_data: list of wind data objects
        :weather_data: list of weather data objects
        :spot_id: The ID of the spot being processed
        :return: List of surf report objects ready to be inserted.
        """

        if not (len(wave_data) == len(wind_data) == len(weather_data)):
            return None

        report_records = []
        for i in range(len(wave_data)):

            report = {
                'spotId': spot_id
            }

            report.update(wave_data[i])
            report.update(wind_data[i])
            report.update(weather_data[i])
            
            report_records.append(report)

        return report_records
    
    def insert_wave_reports(self, db: Database, reports: list, spot_id: str):
        """
        Bulk insert wave reports into mongo db.

        :db: The mongo db - surf
        :reports: List of reports to be inserted
        :spot_id: The ID of the spot the reports are for
        """
        # remove all spot report records that exist
        result = db['WaveReports'].delete_many({ "spotId": spot_id })
        print(f'{result.deleted_count} old reports deleted for spot: {spot_id}')

        # bulk insert the new reports for this spot
        result = db['WaveReports'].insert_many(reports)
        print(f'{len(result.inserted_ids)} new reports inserted for spot: {spot_id}')


    def remove_old_data(self):

        print('removing data')

def main():
    
    # Connect to db
    print('Connecting to database client...')
    client = MongoClient(DB_URI, tlsCAFile=ca)
    print('Connecting to database:', 'surf')
    db = client['surf']

    # Grab all the spots
    spots = db['Spots'].find({}, {
        'spotId': 1
    })
    
    # Get all the spot_ids in a list
    spot_ids = []
    for spot in spots:
        spot_ids.append(spot['spotId'])

    processor = ReportProcessor()
    errors = processor.process_spots(db, spot_ids)

    if len(errors) == 0:
        print(f'All surf reports have been updated!')
    else:
        print('Error: Not all spots were updated')
        pprint(errors)


if __name__ == "__main__":
    main()