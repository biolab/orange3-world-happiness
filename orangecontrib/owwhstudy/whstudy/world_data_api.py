# -----------------------------------------------------------
# World Indicators library for studying the relationship between
# economic indicators and country happiness.
#
# Author: Nejc Hirci
# -----------------------------------------------------------

from pprint import pprint
import wbgapi as wb
import pandas as pd
from pymongo import MongoClient, ReplaceOne
from Orange.util import dummy_callback
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
        cursor = self.db.countries.find({}, {'_id': 1, 'name': 1})
        out = []
        for doc in cursor:
            out.append((doc['_id'], doc['name']))
        return out

    def years(self):
        """ Function gets data from local database.
        :return: list of years with data
        """

        # TODO Think of a more sensible implementation
        cursor = self.db.countries.find({}, {"indicators": 1})
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

    def data(self, countries, indicators, year, skip_empty_columns=True,
             skip_empty_rows=True, include_country_names=False, callback=dummy_callback):
        """ Function gets data from local database.
        :param callback: callback function
        :param include_country_names: add collumn with country names
        :param skip_empty_rows: skip all NaN columns
        :param skip_empty_columns: skip all NaN rows
        :param countries: list of country codes
        :type countries: list
        :param indicators: list of indicator codes
        :type indicators: list
        :param year: year for data
        :type year: list(int) or int
        :return: Pandas dataframe
        """

        if type(year) is int:
            year = [year]

        cols = ["Country name"] if include_country_names else []

        if len(year) > 1:
            for i in indicators:
                for y in year:
                    cols.append(f"{y}-{i}")
        else:
            cols.extend(indicators)

        # Create appropriate pandas Dataframe
        df = pd.DataFrame(data=None, index=countries, columns=cols, dtype=float)

        # Convert country row to string
        if include_country_names:
            df = df.astype({"Country name": str})

        steps = len(countries)
        step = 1

        callback(0, "Fetching data ...")

        # Fill Dataframe from local database
        collection = self.db.countries
        for country in countries:
            doc = collection.find_one({"_id": country})
            if include_country_names:
                df.at[doc['_id'], "Country name"] = doc['name']
            for i in indicators:
                if i in doc['indicators']:
                    values = doc['indicators'][i]
                    if len(year) > 1:
                        for y in year:
                            if str(y) in values:
                                df.at[doc['_id'], f"{y}-{i}"] = values[str(y)]
                    else:
                        if str(year[0]) in values:
                            df.at[doc['_id'], i] = values[str(year[0])]
            callback(step / steps)
            step += 1

        if skip_empty_rows:
            df = df.dropna(axis=0, how='all')

        if skip_empty_columns:
            df = df.dropna(axis=1, how='all')

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

        # Create indicator documents if they don't exist
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

            # For performance reasons split queries on countries and limit to 200 indicators per query
            for country_code in countries:
                doc = self.db.countries.find_one({"_id": country_code})

                if doc is None:
                    doc = {
                        "_id": country_code,
                        "name": find_country_name(country_code),
                        "indicators": {}
                    }

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

                # Update document in remote mongo database
                result = self.db.countries.replace_one({"_id": country_code}, doc)
                print("Data replaced with id", result)

        elif db == 'WHR':
            df = pd.read_csv(f'../data/WHR2021_data_panel.csv')
            df = df.set_index('Country name')

            for country_code in countries:
                doc = self.db.countries.find_one({"_id": country_code})

                if doc is None:
                    doc = {
                        "_id": country_code,
                        "name": find_country_name(country_code),
                        "indicators": {}
                    }

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

                # Update document in remote mongo database
                result = self.db.countries.replace_one({"_id": country_code}, doc)
                print("Data replaced with id", result)


