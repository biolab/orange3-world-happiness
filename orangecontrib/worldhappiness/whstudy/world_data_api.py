# -----------------------------------------------------------
# World Indicators library for studying the relationship between
# economic indicators and country happiness.
#
# Author: Nejc Hirci
# -----------------------------------------------------------


import wbgapi as wb
import json
import numpy as np
import pandas as pd
from requests import HTTPError

import datetime
from pymongo import MongoClient
from Orange.util import dummy_callback

MONGODB_HOST = 'cluster0.vxftj.mongodb.net'
MONGODB_PORT = 27017
DB_NAME = 'world-database'


def find_country_name(name):
    # Needed only for update not addon
    return name


def find_country_alpha2(name):
    # Needed only for update not addon
    return name


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
        years = [str(y) for y in range(2021, 1960, -1)]
        return years

    def indicators(self):
        """ Function gets data from local database.
        Indicator is of form (db, code, is_relative, desc) possibly with url explanation.
        :return: list of indicators
        """
        cursor = self.db.indicators.find({})
        out = []
        for doc in cursor:
            indic = [
                doc['db'],
                str.replace(doc['_id'], '_', '.'),
                doc['desc'],
                doc['code_exp'] if 'code_exp' in doc else [],
                doc['is_relative'] if 'is_relative' in doc else '',
                doc['url'] if 'url' in doc else '',
                doc['sparse_indicator']
            ]
            out.append(tuple(indic))
        return out

    def data(self, countries, indicators, year, include_country_names=True, callback=dummy_callback, index_freq=0,
             country_freq=0):
        """ Function gets data from local database.
        :param country_freq: percentage of not NaN values to keep country
        :param index_freq: percentage of not NaN values to keep indicator
        :param callback: callback function
        :param include_country_names: add collumn with country names                               
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

        steps = len(countries) * len(indicators)
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
                get_last_available_year = True
                if code in doc['indicators']:
                    values = doc['indicators'][code]
                    if len(year) > 1:
                        for y in year:
                            if str(y) in values:
                                df.at[doc['_id'], f"{y}-{i}"] = values[str(y)]
                                get_last_available_year = False
                    else:
                        if str(year[0]) in values:
                            df.at[doc['_id'], i] = values[str(year[0])]
                            get_last_available_year = False

                    if get_last_available_year and len(values) > 0:
                        last_year = list(values.keys())[-1]
                        name = i
                        if type(year) is list:
                            name = f"{last_year}-{i}"
                        df.at[doc['_id'], name] = values[str(last_year)]

                callback(step / steps * 0.8, "Fetching data ...")
                step += 1

        # Remove indicator based on percantage of NaN countries
        min_count = max(len(countries) * index_freq * 0.01, 1)
        df = df.dropna(thresh=min_count, axis=1)

        # Remove country based on percentage of NaN indicators                                                      
        min_count = max(len(df.columns) * country_freq * 0.01, 1)
        df = df.dropna(thresh=min_count, axis=0)

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

        if db == 'WDI':
            wb.db = 2  # Set to WBD/WDI

            # Create indicator documents if they don't exist
            for code in indicators:
                indic_code = str.replace(code, ".", "_")
                if len(list(self.db.indicators.find({"_id": indic_code}).limit(1))) == 0:
                    desc = find_indicator_desc(code, db)
                    doc = {
                        "_id": indic_code,
                        "db": db,
                        "code_exp": [],
                        "desc": desc,
                        "is_relative": '%' in desc,
                        "url": f"https://data.worldbank.org/indicator/{code}"
                    }
                    self.db.indicators.insert_one(doc)

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
                    print(f"[{datetime.datetime.now()}] Updating indicators {i}:{i + 200} for " +
                          f"{doc['name']}")
                    wb_data = wb.data.DataFrame(indicators[i:i + 200], economy=country_code, time=years)
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
            # Instead of list of indicator codes we are sending full_documents

            # Create indicator documents if they don't exist
            for document in indicators:
                indic_code = str.replace(document['_id'], ".", "_")
                if len(list(self.db.indicators.find({"_id": indic_code}).limit(1))) == 0:
                    doc = {
                        "_id": indic_code,
                        "db": document['db'],
                        "code_exp": document['code_exp'],
                        "desc": document['desc'],
                        "is_relative": False,
                        "url": None
                    }
                    self.db.indicators.insert_one(doc)

            countries_str = "+".join(countries)

            # Get the requested data with pandasdmx
            uis = sdmx.Request('OECD')
            big_df = None

            for doc in indicators:
                dataset_id = doc['db'].split("_")[0]
                try:
                    oecd_data = uis.data(
                        resource_id=dataset_id,
                        key=f"{countries_str}.{doc['query_code']}",
                        params={"startTime": years[-1], "endTime": years[0]}
                    )
                    series = sdmx.to_pandas(oecd_data)
                    series = series.droplevel([1, 3, 4, 5, 6])
                    if big_df is None:
                        big_df = pd.DataFrame(series)
                    else:
                        big_df = pd.concat([big_df, pd.DataFrame(series)])

                except HTTPError:
                    print("No Results found for: ", dataset_id)
                    print(f"\t {countries_str}")
                    print(f"\t {doc['query_code']}")

            # Next we will load the data to database
            for country_code in countries:
                doc = self.db.countries.find_one({"_id": country_code})

                if doc is not None:
                    num = 0
                    for indic_doc in indicators:
                        ref_indic_code = indic_doc['query_code'].split(".")[1]
                        indic_code = indic_doc['_id']

                        if indic_code in doc['indicators']:
                            indic = doc['indicators'][indic_code]
                        else:
                            doc['indicators'][indic_code] = {}
                            indic = doc['indicators'][indic_code]

                        y_count = 0
                        for year in years:
                            try:
                                val_loc = big_df.loc[country_code, ref_indic_code, str(year)]
                                indic.update({str(year): val_loc.values[0][0]})
                                y_count += 1
                            except KeyError:
                                pass
                        num += (1 < y_count)
                    print(f"Updated {num}/{len(indicators)} indicators for {country_code}")

                    # Update document in remote mongo database
                    result = self.db.countries.replace_one({"_id": country_code}, doc)
                    print("Data replaced with id", result)

        elif db == 'EVS/WVS':
            # Indicator includes code and description
            # Create indicator documents if they don't exist
            for indic in indicators:
                # Create normal indicator and weighted indicator
                indic_code = str.replace(indic['table_name'], ".", "_")
                if len(list(self.db.indicators.find({"_id": indic_code}).limit(1))) == 0:
                    doc = {
                        "_id": indic_code,
                        "db": db,
                        "code_exp": [],
                        "desc": indic['desc'],
                        "is_relative": True,
                        "url": "",
                        "sparse_indicator": False
                    }
                    self.db.indicators.insert_one(doc)

            for country_code in countries:
                alpha2_code = find_country_alpha2(country_code)
                doc = self.db.countries.find_one({"_id": country_code})

                if doc is None:
                    doc = {
                        "_id": country_code,
                        "name": find_country_name(country_code),
                        "indicators": {}
                    }

                # Load data from csv
                evs_wvs_data = pd.read_csv('../data/evs/EVS_WVS_Joint_csv_v3_0.csv', low_memory=False)
                base_countries = evs_wvs_data['cntry_AN'].to_numpy().flatten()
                unique_countries, idx = np.unique(base_countries, return_index=True)
                country_list = base_countries[np.sort(idx)]

                if alpha2_code in country_list:
                    country_df = evs_wvs_data[evs_wvs_data['cntry_AN'] == alpha2_code]
                    # Get year of country
                    year = list(country_df['year'])[0]

                    for indic in indicators:
                        indic_key = indic['table_name']
                        indicator_code = str.replace(indic_key, '.', '_')
                        if indicator_code in doc['indicators']:
                            indic_doc = doc['indicators'][indicator_code]
                        else:
                            doc['indicators'][indicator_code] = {}
                            indic_doc = doc['indicators'][indicator_code]

                        weighted = False
                        if indic_key[-2:] == '_W':
                            weighted = True
                            indic_key = indic_key[:-2]

                        indic_data = country_df[indic_key]
                        gwght_weights = country_df['gwght']

                        if indic_data.dtypes == 'int64' or indic_data.dtypes == 'float64':
                            if weighted:
                                weighted_data = indic_data * gwght_weights
                                calc_avg = weighted_data.loc[weighted_data >= 0].mean()
                            else:
                                calc_avg = indic_data.loc[indic_data >= 0].mean()
                            indic_doc.update({str(year): float(calc_avg)})
                        else:
                            print(f"Indicator {indic_key} is not a value!")

                    # Update document in remote mongo database
                    print(f"Completed country {country_code} {year}")
                    self.db.countries.replace_one({"_id": country_code}, doc)
                else:
                    f"Skipping {country_code} beacuse missing in file."

            print("FINISHED")


if __name__ == "__main__":
    print("Blank")
