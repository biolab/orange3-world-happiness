"""
Module for world data fetching and loading.
"""
from typing import List

from Orange.data import Table


from orangecontrib.owwhstudy.whstudy.world_data_api import WorldIndicators

class AggregationMethods:
    """
    Aggregation methods enum and helper functions.
    """
    MEAN, MEDIAN, MIN, MAX = range(4)
    ITEMS = "Mean", "Median", "Min", "Max"

    @staticmethod
    def aggregate(
            world_data: Table,
            agg_method: int
    ) -> Table:
        """
        Aggregate scores.

        Parameters
        ----------
        world_data : Table
            Table with data of countries for each indicator and year.
        agg_method : int
            Method type. One of: MEAN, MEDIAN, MIN, MAX.

        Returns
        -------
        Aggregated indicator values by year.
        """
        return [AggregationMethods.mean,
                AggregationMethods.median,
                AggregationMethods.min,
                AggregationMethods.max][agg_method](world_data)

    @staticmethod
    def mean(
            world_data: Table
    ) -> Table:
        """
        'mean' aggregation function.

        Parameters
        ----------
        world_data : list
            Table with data of countries for each indicator and year.

        Returns
        -------
        Aggregated indicator values by year.
        """
        return world_data

    @staticmethod
    def median(
            world_data: Table
    ) -> Table:
        """
        'median' aggregation function.

        Parameters
        ----------
        world_data : list
            Table with data of countries for each indicator and year.

        Returns
        -------
        Aggregated indicator values by year.
        """
        return world_data


    @staticmethod
    def min(
            world_data: Table
    ) -> Table:
        """
        'min' aggregation function.

        Parameters
        ----------
        world_data : list
            Table with data of countries for each indicator and year.

        Returns
        -------
        Aggregated indicator values by year.
        """
        return world_data


    @staticmethod
    def max(
            world_data: Table
    ) -> Table:
        """
        'max' aggregation function.

        Parameters
        ----------
        world_data : list
            Table with data of countries for each indicator and year.

        Returns
        -------
        Aggregated indicator values by year.
        """
        return world_data
