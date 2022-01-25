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
    else:
        return ""


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

        cursor = self.db.countries.find({}, {"indicators": 1})
        years = set()
        for doc in cursor:
            for _, val in doc['indicators'].items():
                for key in val.keys():
                    years.add(key)
        return list(years)

    def indicators(self):
        """ Function gets data from local database.
        Indicator is of form (db, code, is_relative, desc) possibly with url explanation.
        :return: list of indicators
        """
        cursor = self.db.indicators.find({})
        out = []
        for doc in cursor:
            out.append((doc['db'], str.replace(doc['_id'], '_', '.'), doc['is_relative'], doc['desc']))
        return out

    def data(self, countries, indicators, year, skip_empty_columns=True,
             skip_empty_rows=True, include_country_names=True, callback=dummy_callback, index_freq=0):
        """ Function gets data from local database.
        :param index_freq: percentage of not NaN values to keep indicator
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
            for i in indicators:
                cols.append(i)

        # Create appropriate pandas Dataframe
        df = pd.DataFrame(data=None, index=countries, columns=cols, dtype=float)
        df.index.name = "Country code"

        # Add country name column
        if include_country_names:
            df = df.astype({"Country name": str})

        steps = len(countries)
        step = 1

        callback(0, "Fetching data ...")

        query_filter = {'_id': 1, 'name': 1}
        for i in indicators:
            code = str.replace(i, '.', '_')
            query_filter[f'indicators.{code}'] = 1

        # Fill Dataframe from local database
        collection = self.db.countries
        for country in countries:
            doc = collection.find_one({"_id": country}, query_filter)
            if include_country_names:
                df.at[doc['_id'], "Country name"] = doc['name']
            for i in indicators:
                # Must change indicator code to underscores because of Mongo naming restrictions
                code = str.replace(i, '.', '_')
                if code in doc['indicators']:
                    values = doc['indicators'][code]
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
            subset = df.columns.difference(["Country name"]) if include_country_names else df
            df = df.dropna(subset=subset, axis=0, how='all')

        if skip_empty_columns:
            df = df.dropna(axis=1, how='all')

        min_count = len(df) * index_freq*0.01
        df = df.dropna(thresh=min_count, axis=1)
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
            indic_code = str.replace(code, ".", "_")
            if db == 'WDI' and len(list(self.db.indicators.find({"_id": indic_code}).limit(1))) == 0:
                desc = find_indicator_desc(code, db)
                doc = {
                    "_id": indic_code,
                    "desc": desc,
                    "is_relative": '%' in desc,
                    "db": db,
                    "url": f"https://data.worldbank.org/indicator/{code}" if db == 'WDI' else None
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

                        # Must change indicator code to underscores because of Mongo naming restrictions
                        indic_code = str.replace(indic_code, '.', '_')

                        if indic_code in doc['indicators']:
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
            for year in years:
                df = pd.read_csv(f'../data/whr/{year}.csv')
                df = df.set_index('Country')

                for country_code in countries:
                    doc = self.db.countries.find_one({"_id": country_code})
                    country_key = doc['name'] if doc else find_country_name(country_code)

                    print(f"[{datetime.datetime.now()}] Updating WHR{year} indicators for " +
                          f"{country_key}")

                    if doc is None:
                        doc = {
                            "_id": country_code,
                            "name": country_key,
                            "indicators": {}
                        }

                    if country_key in df.index:
                        for indic_key in indicators:
                            indicator_code = str.replace(indic_key, '.', '_')

                            if indic_key in df.columns:
                                if indicator_code in doc['indicators']:
                                    indic = doc['indicators'][indicator_code]
                                else:
                                    doc['indicators'][indicator_code] = {}
                                    indic = doc['indicators'][indicator_code]

                                val = df.at[country_key, indic_key]
                                val = float(val.replace(',', '.')) if isinstance(val, str) else val
                                indic.update({str(year): float(val)})
                            else:
                                print(f"Skipping {indic_key} because missing in file.")

                        # Update document in remote mongo database
                        self.db.countries.replace_one({"_id": country_code}, doc)
                    else:
                        f"Skipping {country_key} beacuse missing in file."

        elif db == 'OECD':
            url = 'http://stats.oecd.org/SDMX-JSON/data/<dataset identifier>/<filter expression>/<agency name>[ ?<additional parameters>]'

if __name__ == "__main__":
    handle = WorldIndicators("main", "biolab")
    indicators = [code for (db, code, _, _) in handle.indicators() if db == 'WHR']
    countries = [code for (code, _) in handle.countries()]
    years = [2021, 2020, 2019, 2018, 2017, 2016, 2015]

    print(countries)
    print(years)
    handle.update(countries, indicators, years, db='WHR')
