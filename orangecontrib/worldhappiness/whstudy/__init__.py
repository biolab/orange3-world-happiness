"""
Module for world data fetching and loading.
"""
from typing import List
import numpy as np
import re

import pandas as pd
from Orange.data import Table, table_from_frame, ContinuousVariable


from orangecontrib.worldhappiness.whstudy.world_data_api import WorldIndicators

GEO_REGIONS = [
    ('AFR', 'Africa',
     {'AGO', 'BDI', 'BEN', 'BFA', 'BWA', 'CAF', 'CIV', 'CMR', 'COD', 'COG', 'COM', 'CPV', 'DJI', 'DZA', 'EGY', 'ERI',
      'ETH', 'GAB', 'GHA', 'GIN', 'GMB', 'GNB', 'GNQ', 'KEN', 'LBR', 'LBY', 'LSO', 'MAR', 'MDG', 'MLI', 'MOZ', 'MRT',
      'MUS', 'MWI', 'NAM', 'NER', 'NGA', 'RWA', 'SDN', 'SEN', 'SLE', 'SOM', 'SSD', 'STP', 'SWZ', 'SYC', 'TCD', 'TGO',
      'TUN', 'TZA', 'UGA', 'ZAF', 'ZMB', 'ZWE'}),
    ('ECS', 'Europe & Central Asia',
     {'ALB', 'AND', 'ARM', 'AUT', 'AZE', 'BEL', 'BGR', 'BIH', 'BLR', 'CHE', 'CHI', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP',
      'EST', 'FIN', 'FRA', 'FRO', 'GBR', 'GEO', 'GIB', 'GRC', 'GRL', 'HRV', 'HUN', 'IMN', 'IRL', 'ISL', 'ITA', 'KAZ',
      'KGZ', 'LIE', 'LTU', 'LUX', 'LVA', 'MCO', 'MDA', 'MKD', 'MNE', 'NLD', 'NOR', 'POL', 'PRT', 'ROU', 'RUS', 'SMR',
      'SRB', 'SVK', 'SVN', 'SWE', 'TJK', 'TKM', 'TUR', 'UKR', 'UZB', 'XKX'}),
    ('EAS', 'East Asia & Pacific',
     {'ASM', 'AUS', 'BRN', 'CHN', 'FJI', 'FSM', 'GUM', 'HKG', 'IDN', 'JPN', 'KHM', 'KIR', 'KOR', 'LAO', 'MAC', 'MHL',
      'MMR', 'MNG', 'MNP', 'MYS', 'NCL', 'NRU', 'NZL', 'PHL', 'PLW', 'PNG', 'PRK', 'PYF', 'SGP', 'SLB', 'THA', 'TLS',
      'TON', 'TUV', 'TWN', 'VNM', 'VUT', 'WSM'}),
    ('LCN', 'Latin America and the Caribbean',
     {'ABW', 'ARG', 'ATG', 'BHS', 'BLZ', 'BOL', 'BRA', 'BRB', 'CHL', 'COL', 'CRI', 'CUB', 'CUW', 'CYM', 'DMA', 'DOM',
      'ECU', 'GRD', 'GTM', 'GUY', 'HND', 'HTI', 'JAM', 'KNA', 'LCA', 'MAF', 'MEX', 'NIC', 'PAN', 'PER', 'PRI', 'PRY',
      'SLV', 'SUR', 'SXM', 'TCA', 'TTO', 'URY', 'VCT', 'VEN', 'VGB', 'VIR'}),
    ('NAC', 'North America', {'BMU', 'CAN', 'USA'}),
    ('SAS', 'South Asia', {'AFG', 'BGD', 'BTN', 'IND', 'LKA', 'MDV', 'NPL', 'PAK'})
]

ORGANIZATIONS = [
    ('OED', 'Organisation for Economic Co-operation and Development',
     {'AUS', 'AUT', 'BEL', 'CAN', 'CHE', 'CHL', 'COL', 'CRI', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GBR',
      'GRC', 'HUN', 'IRL', 'ISL', 'ISR', 'ITA', 'JPN', 'KOR', 'LTU', 'LUX', 'LVA', 'MEX', 'NLD', 'NOR', 'NZL', 'POL',
      'PRT', 'SVK', 'SVN', 'SWE', 'TUR', 'USA'}),
    ('ASEAN', 'Association of Southeast Asian Nations',
     {'BRN', 'KHM', 'IDN', 'LAO', 'MMR', 'MYS', 'PHL', 'SGP', 'THA', 'VNM'}),
    ('NATO', 'North Atlantic Treaty Organization',
     {'ALB', 'BEL', 'BGR', 'CAN', 'HRV', 'CZE', 'DNK', 'EST', 'FRA', 'DEU', 'GRC', 'HUN', 'ISL', 'ITA', 'LVA', 'LTU',
      'LUX', 'MNE', 'NLD', 'MKD', 'NOR', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'TUR', 'GBR', 'USA'}),
    ('EUU', 'European Union',
     {'AUT', 'BEL', 'BGR', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GRC', 'HRV', 'HUN', 'IRL', 'ITA',
      'LTU', 'LUX', 'LVA', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'SWE'})
]


class AggregationMethods:
    """
    Aggregation methods enum and helper functions.
    """
    NONE, MEAN, MEDIAN, MIN, MAX = range(5)
    ITEMS = "None", "Mean", "Median", "Min", "Max"

    @staticmethod
    def aggregate(
            world_data: Table,
            agg_method: int,
            callback,
            index_freq=1,
            country_freq=1,
    ) -> Table:
        """
        Aggregate scores.

        Parameters
        ----------
        world_data : Table
            Table with data of countries for each indicator and year
        agg_method : int
            Method type. One of: MEAN, MEDIAN, MIN, MAX.
        country_freq: float
            Percentage of not NaN values to keep country
        index_freq: float
            Percentage of not NaN values to keep indicator
        callback: callback function

        Returns
        -------
        Aggregated indicator values by year.
        """
        agg_functions = [0, np.nanmean, np.nanmedian, np.nanmax, np.nanmin]

        if agg_method == AggregationMethods.NONE:
            return world_data
        else:
            callback(0.8, 'Aggregating data ...')
            x_df, _, m_df = world_data.to_pandas_dfs()
            cols = []
            for i in world_data.domain.attributes:
                name = i.name.split('-')[1]
                if name not in cols:
                    cols.append(name)

            countries = list(m_df['Country code'])
            df = pd.DataFrame(data=None, index=countries, columns=cols, dtype=float)

            row_count = world_data.X_df.shape[0]
            for step in range(row_count):
                callback(0.8 + 0.2 * step / row_count, 'Aggregating data ...')
                ind_step = 0
                for indicator in cols:
                    selection = x_df.iloc[step, :].filter(regex=indicator)
                    df.iloc[step, ind_step] = selection.agg(agg_functions[agg_method])
                    ind_step += 1

            if m_df.shape[1] > 1:
                df.insert(loc=0, column='Country name', value=list(m_df['Country name']))

            # Remove indicator based on percantage of NaN countries
            min_count = max(row_count * index_freq * 0.01, 1)
            df = df.dropna(thresh=min_count, axis=1)

            # Remove country based on percentage of NaN indicators
            min_count = max(len(cols) * country_freq * 0.01, 2)
            df = df.dropna(thresh=min_count, axis=0)

            return table_from_frame(df)
