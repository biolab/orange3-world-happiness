# World Happiness Study API

This is an implementation of python of API for access to remote Mongo database for a world happiness study.
The data was collected from World Data Bank and World Happiness Reports.

## Installation

Install requirements from `requirements.txt` file:

```shell
python -m pip install .
```

## Usage
```python
# First import script and create class object.
from whstudy import WorldIndicators

# To use already prepared mongo database
db = WorldIndicators('main', 'biolab')
  
# To create a list of indicators and list of countries for further queries.

wdb_indicators = [code for code, desc, db, url in db.indicators() if db == 'WDB']
whr_indicators = [code for code, desc, db, url in db.indicators() if db == 'WHR']
code_countries = [code for code, fullname in db.countries()]

# To get data from remote database in a Pandas dataframe format, where rows consist of 
# country codes and columns consist of `YYYY-indicator_code` if more then one year is 
# requested otherwise indicator_code.
    
# To get wdb_indicators
dataframe = db.data(code_countries, wdb_indicators, list(range(2000,2020)))

# To update data in the remote database you can use.

db.update(code_countries, wdb_indicators, list(range(1960,2020)), 'WDB')
db.update(code_countries, whr_indicators, list(range(1960, 2020)), 'WHR')
```
