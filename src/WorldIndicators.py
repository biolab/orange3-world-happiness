# -----------------------------------------------------------
# World Indicators library for studying the relationship between
# economic indicators and country happiness.
#
# Author: Nejc Hirci
# -----------------------------------------------------------

from pprint import pprint
import wbgapi as wb
import pandas as pd
from pymongo import MongoClient
from pymongo import UpdateOne

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DB_NAME = 'world-indicators'


def find_country_name(code):
    economy = list(wb.economy.list(code))
    if len(economy) == 0:
        raise ValueError(f"Invalid country code {code}.")
    return economy[0]['value']


def find_indicator_desc(code):
    indicator = list(wb.series.list(code))
    if len(indicator) == 0:
        raise ValueError(f"Invalid indicator code {code}.")

    return indicator[0]['value']


class WorldIndicators:

    def __init__(self, user, password):
        self.user = user
        self.pwd = password
        self.db = self.get_connection()

    def get_connection(self):
        """ Set up connection to local mongoDB database
        :return: database object
        """
        uri = f"mongodb://{self.user}:{self.pwd}@{MONGODB_HOST}:{MONGODB_PORT}/?authSource={DB_NAME}&authMechanism" \
              f"=SCRAM-SHA-256"
        client = MongoClient(uri)
        return client[DB_NAME]

    def countries(self):
        """ Function gets data from local database.
        :return: list of countries with country codes and names
        """
        cursor = self.db.countries.find({})
        out = []
        for doc in cursor:
            out.append((doc['_id'], doc['name']))
        return out

    def years(self):
        """ Function gets data from local database.
        :return: list of years with data
        """

        # TODO Think of a more sensible implementation
        cursor = self.db.countries.find({})
        years = []
        for doc in cursor:
            for _, val in doc['indicators'].items():
                for key in val.keys():
                    if key not in years:
                        years.append(key)
        return years

    def indicators(self):
        """ Function gets data from local database.
        Indicator is of form (id, desc, db, home) possibly with url explanation.
        :return: list of indicators
        """
        cursor = self.db.indicators.find({})
        out = []
        for doc in cursor:
            out.append((doc['_id'], doc['desc'], doc['db'], doc['url']))
        return out

    def data(self, countries, indicators, year):
        """ Function gets data from local database.
        :param countries: list of country codes
        :type countries: list
        :param indicators: list of indicator codes
        :type indicators: list
        :param year: year for data
        :type year: list or int
        :return: list of indicators
        """

        cols = indicators
        if type(year) is list and len(year) > 1:
            cols = []
            for y in year:
                for i in indicators:
                    cols.append(f"{y}-{i}")

        # Create appropriate pandas Dataframe
        df = pd.DataFrame(data=None, index=countries, columns=cols)

        # Fill Dataframe from local database
        collection = self.db.countries
        for doc in collection.find({"_id": {"$in": countries}}):
            for i in indicators:
                values = doc['_id']['indicators'][i]
                if type(year) is list and len(year) > 1:
                    for y in year:
                        df.at[doc['_id']][f"{y}-{i}"] = values[str(y)]
                else:
                    df.at[doc['_id']][i] = values[str(year)]
        return df

    def update(self, countries, indicators, years, db):
        """ Refreshes the local database from a given db database.
        :param countries: list of country codes
        :type countries: list
        :param indicators: list of indicator codes
        :type indicators: list
        :param years: list of years
        :type years: list
        :param db: database
        :type db: str
        :return: list of indicators
        """

        # First get country documents if they don't exist create them
        documents = {}
        collection = self.db.countries
        for code in countries:
            name = find_country_name(code)
            if len(list(collection.find({"_id": code}).limit(1))) == 0:
                doc = {
                    "_id": code,
                    "name": name,
                    "indicators": {}
                }
            else:
                doc = collection.find_one({"_id": code})
            documents.update({code: doc})

        # Second create indicator documents if they don't exist
        for code in indicators:
            if len(list(self.db.indicators.find({"_id": code}).limit(1))) == 0:
                doc = {
                    "_id": code,
                    "desc": find_indicator_desc(code),
                    "db": db,
                    "url": None
                }
                self.db.indicators.insert_one(doc)

        if db == 'WDI' or db == 'WBD':
            # Fetch data from WBD
            wb.db = 2
            data = wb.data.DataFrame(indicators, economy=countries, time=years)
            data.reset_index(inplace=True)
            data_dict = data.to_dict("records")

            # Select mongo collection of countries
            collection = self.db.countries

            # Get data and update local python object
            for row in data_dict:
                doc = documents[row['economy']]
                print(f"Update document for country {row['economy']} for {row['series']}")

                if hasattr(doc['indicators'], row['series']):
                    indicator = doc['indicators'][row['series']]
                else:
                    doc['indicators'][row['series']] = {}
                    indicator = doc['indicators'][row['series']]

                for key, val in row.items():
                    if 'YR' in key:
                        indicator.update({key[2:]: val})

            # Update documents in local mongo database
            operations = []
            for _, doc in documents.items():
                operations.append(UpdateOne({"_id": doc['_id']}, {"$set": doc}, upsert=True))

            result = collection.bulk_write(operations)
            pprint(result.bulk_api_result)

        elif db == 'WHR':
            # TODO Implement WHR update function
            pass


if __name__ == "__main__":
    data = WorldIndicators("main", "biolab")

    # Test querry
    data.update(['USA', 'CAN'], ['NY.GDP.PCAP.CD', 'SP.POP.TOTL'], list(range(1920, 2020)), 'WDI')
