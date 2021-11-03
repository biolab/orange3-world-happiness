# -----------------------------------------------------------
# World Indicators library for studying the relationship between
# economic indicators and country happiness.
#
# Author: Nejc Hirci
# -----------------------------------------------------------

import wbgapi as wb
import pandas as pd
from pymongo import MongoClient
import datetime

MONGODB_HOST = 'cluster0.vxftj.mongodb.net'
MONGODB_PORT = 27017
DB_NAME = 'world-database'


def find_country_name(code):
    economy = list(wb.economy.list(code))
    if len(economy) == 0:
        raise ValueError(f"Invalid country code {code}.")
    return economy[0]['value']


def find_indicator_desc(code, db):
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
                  ('LOG.GDP.PER.CAP', 'Logged GDP per capita'),
                  ('SOC.SUP', 'Social support'),
                  ('HEA.LIF.EXP', 'Healthy life expectancy'),
                  ('FRE.LIF.CHO', 'Freedom to make life choices'),
                  ('GEN.SCORE', 'Generosity'),
                  ('PER.OF.COR', 'Perceptions of corruption')
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
        uri = f"mongodb+srv://{self.user}:{self.pwd}@{MONGODB_HOST}/{DB_NAME}?retryWrites=true&w=majority"
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
            if len(list(collection.find({"_id": code}).limit(1))) == 0:
                doc = {
                    "_id": code,
                    "name": find_country_name(code),
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

        if db == 'WDI':
            wb.db = 2  # Set to WBD/WDI
            collection = self.db.countries  # Select mongo collection of countries

            # For performance reasons split queries on countries and limit to 200 indicators per query

            for country_code in countries:
                doc = documents[country_code]

                for i in range(0, len(indicators), 200):
                    print(f"[{datetime.datetime.now()}] Updating indicators {i}:{i+200} for " +
                          f"{doc['name']}")
                    wb_data = wb.data.DataFrame(indicators[i:i+200], economy=country_code, time=years)
                    wb_data.reset_index(inplace=True)
                    data_dict = wb_data.to_dict("records")

                    # Get data and update local python object
                    for row in data_dict:
                        indic_code = indicators[i]
                        if len(indicators) > 1:
                            indic_code = row['series']

                        if hasattr(doc['indicators'], indic_code):
                            indic = doc['indicators'][indic_code]
                        else:
                            doc['indicators'][indic_code] = {}
                            indic = doc['indicators'][indic_code]

                        for key, val in row.items():
                            if 'YR' in key and not pd.isna(val):
                                indic.update({key[2:]: val})

                    # Perform mongo database update
                    collection.update_one({"_id": doc['_id']}, {"$set": doc}, upsert=True)

        elif db == 'WHR':
            df = pd.read_csv(f'./WHR2021_data_panel.csv')
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
                            indic = doc['indicators'][indicator_code]
                        else:
                            doc['indicators'][indicator_code] = {}
                            indic = doc['indicators'][indicator_code]

                        for y in years:
                            if y not in country_df.index:
                                print(f'Year {y} for {country_key} of {indic_key} is missing.')
                            else:
                                val = country_df.at[y, indic_key]
                                indic.update({str(y): val})
                    else:
                        print(f"Skipping {indic_key} because missing in file.")

                # Perform mongo database update
                collection.update_one({"_id": doc['_id']}, {"$set": doc}, upsert=True)


# TODO: Hierarhija za lažje iskanje indikatorjev z metadato


if __name__ == "__main__":
    data = WorldIndicators("main", "biolab")

    '''
    # To update all data from WDI in remote database
    country_codes = [c for c, _ in data.countries()]
    whi_indicators = [i for i, _, db, _ in data.indicators() if db == 'WDI']
    
    data.update(country_codes, whi_indicators, list(range(1960, 2020)), 'WDI'
    '''

    '''
    # To update data from WHR 2021 in remote database
    country_codes = [c for c, _ in data.countries()]
    
    data.update(country_codes, whr_indicators, list(range(1960, 2020)), 'WHR')
    
    '''

    country_codes = [c for c, _ in data.countries()]
    whi_indicators = [i for i, _, db, _ in data.indicators() if db == 'WDI']

    data.update(country_codes, whi_indicators, list(range(1960, 1980)), 'WDI')