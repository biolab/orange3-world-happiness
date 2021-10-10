# -----------------------------------------------------------
# World Indicators library for studying the relationship between
# economic indicators and country happiness.
#
# Author: Nejc Hirci
# -----------------------------------------------------------

import wbgapi as wb
from pymongo import MongoClient


MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DB_NAME = 'world_happiness_indicators'

class WorldIndicators:

    def __init__(self, user, password):
        self.user = user
        self.pwd = password
        self.db = self.get_connection()

    def get_connection(self):
        """ Set up connection to local mongoDB database
        :return: database object
        """
        client = MongoClient("mongodb://{}:{}@{}:{}/".format(self.user, self.pwd, MONGODB_HOST, MONGODB_PORT))
        return client[DB_NAME]

    def countries(self):
        """ Function gets data from local database.
        :return: list of countries with country codes and names
        """
        return list()


    def years(self):
        """ Function gets data from local database.
        :return: list of years with data
        """
        return list()


    def indicators(self):
        """ Function gets data from local database.
        Indicator is of form (id, desc, db, home) possibly with url explanation.
        :return: list of indicators
        """
        return list()


    def data(self, countries, indicators, year):
        """ Function gets data from local database.
        :param countries: list of country codes
        :type countries: list
        :param indicators: list of indicator codes
        :type indicators: list
        :param year: year for data
        :type year: int
        :return: list of indicators
        """
        return list()


    def data(self, countries, indicators, year):
        """ Function gets data from local database.
        :param countries:
        :param indicators:
        :param year: list of years
        :type year: list
        :return: list of indicators
        """


    def update(self, countries, indicators, year, db):
        """ Refreshes the local database from a given db database.
        :param countries: list of country codes
        :type countries: list
        :param indicators: list of indicator codes
        :type indicators: list
        :param year: year for data
        :type year: int
        :param db: database
        :type db: str
        :return: list of indicators
        """

        if db == "WDI":

        elif db == "WHR":


        # TODO: Optimization depending on database interface using multiple calls

        return list()


if __name__ == "__main__":
    data = WorldIndicators("admin", "biolab")
    print(data.db)