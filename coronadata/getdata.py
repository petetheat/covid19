import pandas as pd
import os

import errno
from datetime import datetime

import matplotlib.pyplot as plt


def get_index(x):
    """
    Defines unique index based on country and province/state name and date

    :param x: Row of inital dataframe
    :return: New unique index, :obj:`string`
    """
    if isinstance(x['Province/State'], str):
        return x['Province/State'] + '_' + x['Country/Region'] + '_' + datetime.strftime(x['Date'], "%m/%d/%Y")
    else:
        return x['Country/Region'] + '_' + datetime.strftime(x['Date'], "%m/%d/%Y")


def melt_df(df):
    """

    :param df:
    :return:
    """
    new_df = pd.melt(df, id_vars=['Province/State', 'Country/Region'],
                     value_vars=df.columns[4:], var_name='Date', value_name='Cases')

    new_df['Date'] = pd.to_datetime(new_df['Date'])
    new_df.sort_values(by=['Country/Region', 'Province/State', 'Date'], inplace=True)

    new_index = new_df.apply(lambda x: get_index(x), axis=1)

    return new_df.reset_index(drop=True).set_index(new_index)


class DataLoader:
    def __init__(self, file_path):
        """

        :param file_path: :obj:`string` path to where the time series csv files are stored.
        """
        files = ['time_series_covid19_confirmed_global.csv', 'time_series_covid19_deaths_global.csv',
                 'time_series_covid19_recovered_global.csv']

        for f in files:
            filename = os.path.join(file_path, f)
            if not os.path.exists(filename):
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), '%s' % filename)

        # Load time series data
        df_confirmed = pd.read_csv(os.path.join(file_path, 'time_series_covid19_confirmed_global.csv'))
        df_death = pd.read_csv(os.path.join(file_path, 'time_series_covid19_deaths_global.csv'))
        df_recovered = pd.read_csv(os.path.join(file_path, 'time_series_covid19_recovered_global.csv'))

        df_total_confirmed = melt_df(df_confirmed)
        df_total_deaths = melt_df(df_death)
        df_total_recovered = melt_df(df_recovered)

        df_total = df_total_confirmed.join(df_total_deaths, lsuffix='_confirmed', rsuffix='_deaths')
        df_total = df_total.join(df_total_recovered, lsuffix='', rsuffix='_recovered')

        df_total.drop(columns=['Province/State_confirmed', 'Province/State_deaths',
                               'Province/State', 'Country/Region', 'Country/Region_deaths', 'Date', 'Date_deaths'],
                      inplace=True)

        # Some countries don't have recovered cases and are NaNs. In order to calculate active cases these are set to
        # zero
        df_total.fillna(0, inplace=True)

        # Rename columns
        df_total.rename(columns={"Country/Region_confirmed": "Country/Region", "Date_confirmed": "Date",
                                 "Cases_confirmed": "Cases confirmed", "Cases_deaths": "Cases deaths",
                                 "Cases": "Cases Recovered"}, inplace=True)

        # Calculate active cases
        df_total['Cases active'] = df_total['Cases confirmed'] - df_total['Cases deaths'] - df_total['Cases Recovered']

        self.df = df_total

    def plot(self, country, **kwargs):
        figsize = kwargs['figsize'] if 'figsize' in kwargs else (10, 5)

        fig, ax = plt.subplots(figsize=figsize)
        self.df.loc[self.df['Country/Region'] == country].plot(x='Date', y=['Cases active', 'Cases confirmed',
                                                                            'Cases Recovered', 'Cases deaths'],
                                                               ax=ax)
        ax.grid()
        ax.set_title(country)




