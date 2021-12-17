"""
Module for world data fetching and loading.
"""
from typing import List
import numpy as np
import re

import pandas as pd
from Orange.data import Table, table_from_frame, ContinuousVariable


from orangecontrib.owwhstudy.whstudy.world_data_api import WorldIndicators


class AggregationMethods:
    """
    Aggregation methods enum and helper functions.
    """
    NONE, MEAN, MEDIAN, MIN, MAX = range(5)
    ITEMS = "None", "Mean", "Median", "Min", "Max"

    @staticmethod
    def aggregate(
            world_data: Table,
            countries: List,
            indicators: List,
            years: List,
            agg_method: int
    ) -> Table:
        """
        Aggregate scores.

        Parameters
        ----------
        world_data : Table
            Table with data of countries for each indicator and year.
        countries : List
            List of countries in results
        indicators : List
            List of indicator codes in results
        years : List
            List of years in results
        agg_method : int
            Method type. One of: MEAN, MEDIAN, MIN, MAX.

        Returns
        -------
        Aggregated indicator values by year.
        """
        return [AggregationMethods.none,
                AggregationMethods.mean,
                AggregationMethods.median,
                AggregationMethods.min,
                AggregationMethods.max][agg_method](world_data, countries, indicators, years)

    @staticmethod
    def none(
            world_data: Table,
            countries: List,
            indicators: List,
            years: List
    ) -> Table:
        """
        'mean' aggregation function.
        Parameters
        ----------
        world_data : list
            Table with data of countries for each indicator and year.
        countries : List
            List of countries in results
        indicators : List
            List of indicator codes in results
        years : List
            List of years in results
        Returns
        -------
        Aggregated indicator values by year.
        """
        return world_data

    @staticmethod
    def mean(
            world_data: Table,
            countries: List,
            indicators: List,
            years: List
    ) -> Table:
        """
        'mean' aggregation function.
        Parameters
        ----------
        world_data : list
            Table with data of countries for each indicator and year.
        countries : List
            List of countries in results
        indicators : List
            List of indicator codes in results
        years : List
            List of years in results
        Returns
        -------
        Aggregated indicator values by year.
        """
        # Construct a new Domain
        cols = []
        if len(world_data.domain.metas) > 1:
            cols.append("Country name")
        for i in indicators:
            cols.append(i)

        df = pd.DataFrame(data=None, index=countries, columns=cols, dtype=float)

        for row in world_data:
            for indicator in indicators:
                values = []
                for year in years:
                    name = f"{year}-{indicator}"
                    if ContinuousVariable(name) in world_data.domain.variables:
                        values.append(row[name])
                df.at[row['index'], indicator] = np.mean(values)
        return table_from_frame(df)

    @staticmethod
    def median(
            world_data: Table,
            countries: List,
            indicators: List,
            years: List
    ) -> Table:
        """
        'median' aggregation function.

        Parameters
        ----------
        world_data : list
            Table with data of countries for each indicator and year.
        countries : List
            List of countries in results
        indicators : List
            List of indicator codes in results
        years : List
            List of years in results
        Returns
        -------
        Aggregated indicator values by year.
        """
        # Construct a new Domain
        cols = []
        if len(world_data.domain.metas) > 1:
            cols.append("Country name")
        for i in indicators:
            cols.append(i)

        df = pd.DataFrame(data=None, index=countries, columns=cols, dtype=float)

        for row in world_data:
            for indicator in indicators:
                values = []
                for year in years:
                    name = f"{year}-{indicator}"
                    if ContinuousVariable(name) in world_data.domain.variables:
                        values.append(row[name])
                df.at[row['index'], indicator] = np.median(values)
        return table_from_frame(df)

    @staticmethod
    def min(
            world_data: Table,
            countries: List,
            indicators: List,
            years: List
    ) -> Table:
        """
        'min' aggregation function.

        Parameters
        ----------
        world_data : list
            Table with data of countries for each indicator and year.
        countries : List
            List of countries in results
        indicators : List
            List of indicator codes in results
        years : List
            List of years in results
        Returns
        -------
        Aggregated indicator values by year.
        """
        # Construct a new Domain
        cols = []
        if len(world_data.domain.metas) > 1:
            cols.append("Country name")
        for i in indicators:
            cols.append(i)

        df = pd.DataFrame(data=None, index=countries, columns=cols, dtype=float)

        for row in world_data:
            for indicator in indicators:
                values = []
                for year in years:
                    name = f"{year}-{indicator}"
                    if ContinuousVariable(name) in world_data.domain.variables:
                        values.append(row[name])
                df.at[row['index'], indicator] = np.min(values)
        return table_from_frame(df)

    @staticmethod
    def max(
            world_data: Table,
            countries: List,
            indicators: List,
            years: List
    ) -> Table:
        """
        'max' aggregation function.

        Parameters
        ----------
        world_data : list
            Table with data of countries for each indicator and year.
        countries : List
            List of countries in results
        indicators : List
            List of indicator codes in results
        years : List
            List of years in results
        Returns
        -------
        Aggregated indicator values by year.
        """
        # Construct a new Domain
        cols = []
        if len(world_data.domain.metas) > 1:
            cols.append("Country name")
        for i in indicators:
            cols.append(i)

        df = pd.DataFrame(data=None, index=countries, columns=cols, dtype=float)

        for row in world_data:
            for indicator in indicators:
                values = []
                for year in years:
                    name = f"{year}-{indicator}"
                    if ContinuousVariable(name) in world_data.domain.variables:
                        values.append(row[name])
                df.at[row['index'], indicator] = np.max(values)
        return table_from_frame(df)
