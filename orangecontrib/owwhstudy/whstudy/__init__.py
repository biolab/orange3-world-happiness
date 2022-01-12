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
            years: List,
            agg_method: int
    ) -> Table:
        """
        Aggregate scores.

        Parameters
        ----------
        world_data : Table
            Table with data of countries for each indicator and year
        years : List
            List of years in results
        agg_method : int
            Method type. One of: MEAN, MEDIAN, MIN, MAX.

        Returns
        -------
        Aggregated indicator values by year.
        """
        agg_functions = [0, np.nanmean, np.nanmedian, np.nanmax, np.nanmin]

        if agg_method == AggregationMethods.NONE:
            return world_data
        else:
            cols = []
            for i in world_data.domain.attributes:
                name = i.name.split('-')[1]
                if name not in cols:
                    cols.append(name)

            countries = list(world_data.metas_df.iloc[:, 0])
            df = pd.DataFrame(data=None, index=countries, columns=cols, dtype=float)

            if world_data.metas.shape[1] > 1:
                df.insert(loc=1, column='Country name', value=list(world_data.metas_df.iloc[:, 1]))

            for row in world_data:
                for indicator in cols:
                    values = []
                    for year in years:
                        name = f"{year}-{indicator}"
                        if ContinuousVariable(name) in world_data.domain.attributes and not np.isnan(row[name]):
                            values.append(row[name])
                    df.at[row['Country code'], indicator] = agg_functions[agg_method](values) if values else np.nan
            return table_from_frame(df)
