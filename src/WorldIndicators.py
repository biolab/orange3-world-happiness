# -----------------------------------------------------------
# World Indicators library for studying the relationship between
# economic indicators and country happiness.
#
# Author: Nejc Hirci
# -----------------------------------------------------------

def countries():
    """ Function gets data from World Bank Data database at https://data.worldbank.org.
    :return: list of countries with country codes and names
    """
    return list()


def years():
    """ Function gets data from World Bank Data database at https://data.worldbank.org.
    :return: list of years with data
    """
    return list()


def indicators():
    """ Function gets data from World Bank Data database at https://data.worldbank.org.
    Indicator is of form (id, desc, db, home) possibly with url explanation.
    :return: list of indicators
    """
    return list()


def data(countries, indicators, year):
    """
    :param countries: list of country codes
    :type countries: list
    :param indicators: list of indicator codes
    :type indicators: list
    :param year: year for data
    :type year: int
    :return: list of indicators
    """
    return list()


def update(countries, indicators, year, db):
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

    # TODO: Optimization depending on database interface using multiple calls

    return list()