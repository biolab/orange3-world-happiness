# Modifications copyright (C) 2022 <Biolab/Nejc Hirci>

import requests
import xmltodict
import pandas as pd
import datetime
import os


if __name__ == "__main__":
    DATA_DIR = '../../data'
    DATAFILE = os.path.join(DATA_DIR, 'OECD_key_names.csv')
    # http://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/ALL
    # Extract OECD KeyFamily id (dataset id) and English description

    # get the data structure schema with the key families (dataset ids)
    dataStructureUrl = 'http://stats.oecd.org/RESTSDMX/sdmx.ashx/GetDataStructure/ALL/'

    try:
        r = requests.get(dataStructureUrl, timeout=61)
    except requests.exceptions.ReadTimeout:
        print("Data request read timed out")
    except requests.exceptions.Timeout:
        print("Data request timed out")
    except requests.exceptions.HTTPError:
        print("HTTP error")
    except requests.exceptions.ConnectionError:
        print("Connection error")
    else:
        if r.status_code == 200:
            keyFamIdList = []
            keyFamNameList = []

            # use xmltodict and traverse nested ordered dictionaries
            keyfamilies_dict = xmltodict.parse(r.text)
            keyFamilies = keyfamilies_dict['message:Structure']['message:KeyFamilies']['KeyFamily']

            for keyFamily in keyFamilies:
                keyfam_id = keyFamily['@id']
                keyFamIdList.append(keyfam_id)
                keyNames = keyFamily['Name']
                if isinstance(keyNames, list):
                    for keyName in keyNames:
                        try:
                            keyfam_lang = keyName['@xml:lang']
                            if keyfam_lang == 'en':
                                keyfam_text = keyName['#text']
                                keyFamNameList.append(keyfam_text)
                                # print(keyfam_id, keyfam_text)
                        except KeyError as e:
                            print("No @xml:lang/#text key in {}".format(keyName))
                elif isinstance(keyNames, dict):
                    try:
                        keyfam_lang = keyNames['@xml:lang']
                        if keyfam_lang == 'en':
                            keyfam_text = keyNames['#text']
                            keyFamNameList.append(keyfam_text)
                            # print(keyfam_id, keyfam_text)
                    except KeyError as e:
                        print("No @xml:lang/#text key in {}".format(keyNames))

            # create a 2D list(needed?), and a data frame. Save data frame to csv file
            # keyFamTable = [keyFamIdList, keyFamNameList]
            keyFamDF = pd.DataFrame({'KeyFamilyId': keyFamIdList, 'KeyFamilyName': keyFamNameList})
            keyFamDF.set_index('KeyFamilyId', inplace=True)
            keyFamDF.to_csv(DATAFILE)
        else:
            print('HTTP Failed with code', r.status_code)

    print("completed ...")


