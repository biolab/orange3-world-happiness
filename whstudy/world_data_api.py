# -----------------------------------------------------------
# World Indicators library for studying the relationship between
# economic indicators and country happiness.
#
# Author: Nejc Hirci
# -----------------------------------------------------------

import wbgapi as wb
import pandas as pd
import re
from requests import HTTPError
import datetime
import pandasdmx as sdmx
import xmltodict
from pymongo import MongoClient
from Orange.util import dummy_callback


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
            indic = [
                doc['db'],
                str.replace(doc['_id'], '_', '.'),
                doc['desc'],
                doc['code_exp'] if 'code_exp' in doc else [],
                doc['is_relative'] if 'is_relative' in doc else '',
                doc['url'] if 'url' in doc else ''
            ]
            out.append(tuple(indic))
        return out

    def data(self, countries, indicators, year, skip_empty_columns=True,
             skip_empty_rows=True, include_country_names=True, callback=dummy_callback,
             index_freq=0, country_freq=0):
        """ Function gets data from local database.
        :param country_freq: percentage of not NaN values to keep country
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

        # Remove indicator based on percantage of NaN countries
        min_count = len(df) * index_freq * 0.01
        df = df.dropna(thresh=min_count, axis=1)

        # Remove country based on percentage of NaN indicators
        min_count = len(df.columns) * country_freq * 0.01
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

        elif 'OECD' == 'OECD':
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


if __name__ == "__main__":
    with open('../data/oecd_schema/HSL.xml', mode='r') as f:
        data = f.read()
        hsl_xml = xmltodict.parse(data)['Structure']['CodeLists']['CodeList']
        variable = hsl_xml[2]['Code']

        locs = []
        for i in hsl_xml[0]['Code']:
            locs.append(i['@value'])

        querry_codes = []
        code_exp = []
        for i in variable:
            if not '_' in i['@value']:
                # General topic
                topic_general = i['Description'][0]['#text']
            else:
                querry_codes.append(i['@value'])
                code_exp.append((topic_general, i['Description'][0]["#text"]))

    topic_codes = [
        ('Income and Wealth', 'IW'),
        ('Work and Job Quality',  'WJQ'),
        ('Housing', 'HH'),
        ('Work-life Balance', 'WLB'),
        ('Health', 'HLT'),
        ('Knowledge and skills', 'KS'),
        ('Social connections', 'SCO'),
        ('Civic Engagement', 'CE'),
        ('Environmental quality', 'EQ'),
        ('Safety', 'SAF'),
        ('Subjective Well-being', 'SWB'),
        ('Natural Capital', 'NC'),
        ('Human Capital', 'HC'),
        ('Social Capital', 'SC'),
        ('Economic Capital', 'EC')
    ]

    new_codes = [('Income and Wealth', 'Household income', 'IW.HI'),
                 ('Income and Wealth', 'Household wealth', 'IW.HW'),
                 ('Income and Wealth', 'Relative income poverty', 'IW.RIP'),
                 ('Income and Wealth', 'Difficulty making ends meet', 'IW.DME'),
                 ('Income and Wealth', 'Financial insecurity', 'IW.FI'),
                 ('Work and Job Quality', 'Employment rate', 'WJQ.ER'),
                 ('Work and Job Quality', 'Gender wage gap', 'WJQ.GWG'),
                 ('Work and Job Quality', 'Long-term unemployment rate', 'WJQ.LUR'),
                 ('Work and Job Quality', 'Youth not in employment, education or training', 'WJQ.YNI'),
                 ('Work and Job Quality', 'Labour market insecurity', 'WJQ.LMI'),
                 ('Work and Job Quality', 'Job strain', 'WJQ.JS'),
                 ('Work and Job Quality', 'Long hours in paid work', 'WJQ.LHI'),
                 ('Work and Job Quality', 'Earnings', 'WJQ.E'),
                 ('Housing', 'Overcrowding rate', 'HH.OR'),
                 ('Housing', 'Housing affordability', 'HH.HA'),
                 ('Housing', 'Housing cost overburden', 'HH.HCO'),
                 ('Housing', 'Poor households without access to basic sanitary facilities', 'HH.PHW'),
                 ('Housing', 'Households with internet access at home', 'HH.HWI'),
                 ('Work-life Balance', 'Time off', 'WLB.TO'),
                 ('Work-life Balance', 'Long unpaid working hours', 'WLB.LUW'),
                 ('Work-life Balance', 'Gender gap in working hours', 'WLB.GGI'),
                 ('Work-life Balance', 'Satisfaction with time use', 'WLB.SWT'),
                 ('Health', 'Life expectancy at birth', 'HLT.LEA'),
                 ('Health', 'Perceived health', 'HLT.PH'),
                 ('Health', 'Deaths from suicide, alcohol, drugs', 'HLT.DFS'),
                 ('Health', 'Self-reported depression', 'HLT.SD'),
                 ('Knowledge and skills', 'Student skills (reading)', 'KS.SSR'),
                 ('Knowledge and skills', 'Student skills (maths)', 'KS.SSM'),
                 ('Knowledge and skills', 'Student skills (science)', 'KS.SSS'),
                 ('Knowledge and skills', 'Adult skills (numeracy)', 'KS.ASN'),
                 ('Knowledge and skills', 'Adult skills (literacy)', 'KS.ASL'),
                 ('Social connections', 'Social support', 'SCO.SS'),
                 ('Social connections', 'Time spent in social interactions', 'SCO.TSI'),
                 ('Social connections', 'Satisfaction with personal relationships', 'SCO.SWP'),
                 ('Civic Engagement', 'Having a say in government', 'CE.HAS'),
                 ('Civic Engagement', 'Voter turnout', 'CE.VT'),
                 ('Environmental quality', 'Access to green space', 'EQ.ATG'),
                 ('Environmental quality', 'Air pollution', 'EQ.AP'),
                 ('Safety', 'Homicides', 'SAF.H'),
                 ('Safety', 'Feeling safe at night', 'SAF.FSA'),
                 ('Safety', 'Road deaths', 'SAF.RD'),
                 ('Subjective Well-being', 'Life satisfaction', 'SWB.LS'),
                 ('Subjective Well-being', 'Negative affect balance', 'SWB.NAB')
    ]

    # Removing empty data from querry_codes
    unusable_codes = ['1_2', '12_*', '13_*', '14_*', '15_*']
    unusable_regex = re.compile('|'.join(unusable_codes))
    usuable_codes = []

    for c in querry_codes:
        if unusable_regex.match(c) is None:
            usuable_codes.append(c)

    new_indicators = []
    for i in range(len(new_codes)):
        new_indicators.append({
            '_id': str.replace(new_codes[i][2], '.', '_'),
            'db': 'HSL_OECD',
            'code_exp': [
                new_codes[i][0],
                new_codes[i][1]
            ],
            'query_code': f'AVERAGE+DEP.{usuable_codes[i]}.CWB.TOT.TOT.TOT',
            'desc': new_codes[i][0]+", "+new_codes[i][1],
            'is_relative': False,
            'url': None
        })

    # HSL request structure
    # COUNTRY.TYPE_OF_INDICATOR.INDICATOR.CURRENT/FUTURE WELL-BEING.SEX.AGE.EDUCATION.TIME
    # TYPE_OF_INDICATOR: AVERAGE, DEP, VER, HOR
    # INDICATOR_CODES: parse
    # WB: CWB, FWB
    # SEX: TOT, FEMALE, MALE
    # AGE: TOT, YOUNG, MIDDLE_AGED, OLD
    # EDUCATION: TOT, PRIMARY, SECONDARY, TERTIARY
    # TIME: 2002 - 2021

    handle = WorldIndicators("main", "biolab")
    years = list(range(2021, 2004, -1))

    country_codes = [code for (code, _) in handle.countries()]
    for loc in locs:
        if loc not in country_codes:
            locs.remove(loc)

    handle.update(locs, new_indicators, years, db='OECD')
