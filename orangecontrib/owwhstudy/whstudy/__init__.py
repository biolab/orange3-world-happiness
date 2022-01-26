"""
Module for world data fetching and loading.
"""
from typing import List
import numpy as np
import re

import pandas as pd
from Orange.data import Table, table_from_frame, ContinuousVariable


from orangecontrib.owwhstudy.whstudy.world_data_api import WorldIndicators

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
