# -----------------------------------------------------------
# World Indicators library for studying the relationship between
# economic indicators and country happiness.
#
# Author: Nejc Hirci
# -----------------------------------------------------------
import os.path
from pprint import pprint
import wbgapi as wb
import pandas as pd
from pymongo import MongoClient
from pymongo import UpdateOne
import csv

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DB_NAME = 'world-indicators'


def find_country_name(code):
    economy = list(wb.economy.list(code))
    if len(economy) == 0:
        raise ValueError(f"Invalid country code {code}.")
    return economy[0]['value']


def find_indicator_desc(code,db):
    if db == 'WDI' or db == 'WDB':
        indicator = list(wb.series.list(code))
        if len(indicator) == 0:
            raise ValueError(f"Invalid indicator code {code}.")

        return indicator[0]['value']
    elif db == 'WHR':
        for item in whr_indicators:
            if item[0] == code:
                return item[1]


whr_indicators = [('HAP.SCORE', 'Ladder score'),
                  ('HAP.SCORE.STD.ERR', 'Standard error of ladder score'),
                  ('LOG.GDP.PER.CAP', 'Logged GDP per capita'),
                  ('SOC.SUP', 'Social support'),
                  ('HEA.LIF.EXP', 'Healthy life expectancy'),
                  ('FRE.LIF.CHO', 'Freedom to make life choices'),
                  ('GEN.SCORE', 'Generosity'),
                  ('PER.OF.COR', 'Perceptions of corruption'),
                  ('HAP.SCORE.DYST', 'Ladder score in Dystopia'),
                  ('EXP.BY.GDP', 'Explained by: Log GDP per capita'),
                  ('EXP.BY.SOC.SUP', 'Explained by: Social support'),
                  ('EXP.BY.HEA.LIF.EXP', 'Explained by: Healthy life expectancy'),
                  ('EXP.BY.FRE.LIF.CHO', 'Explained by: Freedom to make life choices'),
                  ('EXP.BY.GEN', 'Explained by: Generosity'),
                  ('EXP.BY.PER.OF.COR', 'Explained by: Perceptions fo corruption'),
                  ('DYST.AND.RES', 'Dystopia with residual')
                  ]


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
            for i in indicators:
                for y in year:
                    cols.append(f"{y}-{i}")

        # Create appropriate pandas Dataframe
        df = pd.DataFrame(data=None, index=countries, columns=cols)

        # Fill Dataframe from local database
        collection = self.db.countries
        for doc in collection.find({"_id": {"$in": countries}}):
            for i in indicators:
                values = doc['indicators'][i]
                if type(year) is list and len(year) > 1:
                    for y in year:
                        if str(y) in values:
                            df.at[doc['_id'], f"{y}-{i}"] = values[str(y)]
                else:
                    if str(year) in values:
                        df.at[doc['_id'], i] = values[str(year)]
        print(df)
        return df

    def update(self, countries, indicators, years, db):
        """ Refreshes the local database from a given db database.
        :param countries: list of country codes
        :type countries: list
        :param indicators: list of indicator codes
        :type indicators: list
        :param years: list of years
        :type years: list or int
        :param db: database
        :type db: str
        :return: list of indicators
        """

        if type(years) is int:
            years = [years]

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
                    "desc": find_indicator_desc(code, db),
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

        elif db == 'WHR':
            df = pd.read_csv(f'../WHR2021_data_panel.csv')
            df = df.set_index('Country name')

            for country_code in countries:
                doc = documents[country_code]
                country_key = find_country_name(country_code)
                country_df = df[df.index == country_key]
                country_df = country_df.set_index('year')

                for indicator_code in indicators:
                    indic_key = find_indicator_desc(indicator_code, db)

                    if indic_key in df.columns:
                        if hasattr(doc['indicators'], indicator_code):
                            indicator = doc['indicators'][indicator_code]
                        else:
                            doc['indicators'][indicator_code] = {}
                            indicator = doc['indicators'][indicator_code]

                        for year in years:
                            if year not in country_df.index:
                                print(f'Year {year} for {country_key} of {indic_key} is missing.')
                            else:
                                print(country_df.index)
                                val = country_df.at[year, indic_key]
                                indicator.update({str(year): val})
                    else:
                        print(f"Skipping {indic_key} because missing in file.")

        # Update documents in local mongo database
        operations = []
        for _, doc in documents.items():
            operations.append(UpdateOne({"_id": doc['_id']}, {"$set": doc}, upsert=True))

        result = collection.bulk_write(operations)
        pprint(result.bulk_api_result)

if __name__ == "__main__":
    data = WorldIndicators("main", "biolab")

    # Test querry for WDB
    # data.update(['USA', 'CAN'], ['NY.GDP.PCAP.CD', 'SP.POP.TOTL'], list(range(1920, 2020)), 'WDI')
    # data.data(['USA', 'CAN'], ['NY.GDP.PCAP.CD', 'SP.POP.TOTL'], list(range(1920, 2020)))

    data.update(['USA'], ['HAP.SCORE'], 2013, 'WHR')
