from whstudy.world_data_api import WorldIndicators
import pandas as pd
import numpy as np

def get_code_names(indic_code, df):
    codes = indic_code.split('_')
    code_names = []
    topic = codes[0]
    general_subject = codes[1]
    specific_subject = codes[2]
    extensions = codes[3:]

    s = df.loc[df['Topic'] == topic, 'Topic description']
    if s.size > 0:
        code_names.append(s.iat[0])
    s = df.loc[df['General subject'] == general_subject, 'General subject description']
    if s.size > 0:
        code_names.append(s.iat[0])
    s = df.loc[df['Specific subject'] == specific_subject, 'Specific subject description']
    if s.size > 0:
        code_names.append(s.iat[0])

    for extension in extensions:
        s = df.loc[df['Extensions'] == extension, 'Extensions description']
        if s.size > 0:
            code_names.append(s.iat[0])

    return code_names

if __name__ == "__main__":
    handle = WorldIndicators("main", "biolab")
    indicators = handle.indicators()

    # df = pd.read_csv('./data/wdi/wdi_cets.csv')
    # df = df.set_index('Series Code')
    df = pd.read_csv('./data/wdi/wdi_short_codes.csv')

    for i in indicators:
        if i[0] == 'WDI':
            indic_code = str.replace(i[1], '.', '_')
            doc = handle.db.indicators.find_one({"_id": indic_code})
            print(indic_code)
            code_names = get_code_names(indic_code, df)
            new_doc = {
                "_id": doc["_id"],
                "db": doc["db"],
                "code_exp": code_names,
                "desc": doc["desc"],
                "is_relative": doc["is_relative"],
                "url": doc["url"]
            }
            handle.db.indicators.replace_one({"_id": indic_code}, new_doc)

