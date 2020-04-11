import pandas as pd
import os

import errno
from datetime import datetime

import matplotlib.pyplot as plt
import requests
from tqdm import tqdm

import math


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


def query_rki(n, results_ids, n_batch):
    result_list = []

    tqdm.write('Querying data from RKI...')
    for b in tqdm(range(n_batch)):
        idx_start = b * n
        idx_end = idx_start + n
        test_ids = results_ids[idx_start:idx_end]
        test_ids = [str(i) for i in test_ids]

        id_query = '%2C+'.join(test_ids)

        query = 'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&objectIds=' + id_query + '&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson&token='

        r = requests.get(query)
        results = r.json()

        result_list.append(results)

    idx_start = n_batch * n
    test_ids = results_ids[idx_start:]
    test_ids = [str(i) for i in test_ids]

    id_query = '%2C+'.join(test_ids)

    query = 'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&objectIds=' + id_query + '&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson&token='

    r = requests.get(query)
    results = r.json()

    result_list.append(results)

    x = []
    tqdm.write('Creating pandas dataframe...')
    for r in tqdm(result_list):
        for f in r['features']:
            x.append(f['attributes'])

    return pd.DataFrame(x)


class get_rki():
    def __init__(self):

        # get object IDs:
        query = 'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=true&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson&token='
        r = requests.get(query)
        results_ids = r.json()['objectIds']

        self.ids = results_ids

        n = 150
        n_batch = math.floor(len(results_ids) / n)

        self.raw_data = query_rki(n, results_ids, n_batch)
        self.raw_data['Meldedatum'] = pd.to_datetime(self.raw_data['Meldedatum'], unit='ms')

    def plot(self, **kwargs):
        figsize = kwargs['figsize'] if 'figsize' in kwargs else (10, 5)

        if 'region' in kwargs:
            region = kwargs['region']

            if region not in self.raw_data['Bundesland'].unique():
                raise ValueError("Unknown region '%s'. "
                                 "Possible values are %s" % (region,
                                                             ', '.join(self.raw_data.Bundesland.unique())))

            df = self.raw_data.groupby(['Bundesland', 'Meldedatum']).sum().groupby('Bundesland').cumsum().reset_index()
            df.rename(columns={'Meldedatum': 'Date', 'AnzahlFall': 'Cases confirmed',
                               'AnzahlTodesfall': 'Cases deaths', 'AnzahlGenesen': 'Cases Recovered',
                               'Bundesland': 'Region'}, inplace=True)
            df['Cases active'] = df['Cases confirmed'] - df['Cases deaths'] - df['Cases Recovered']
        else:
            df = self.raw_data.groupby(['Meldedatum']).sum()
            df.reset_index(inplace=True)
            df['Cases confirmed'] = df['AnzahlFall'].cumsum()
            df['Cases deaths'] = df['AnzahlTodesfall'].cumsum()
            df['Cases Recovered'] = df['AnzahlGenesen'].cumsum()
            df['Cases active'] = df['Cases confirmed'] - df['Cases deaths'] - df['Cases Recovered']
            df.rename(columns={'Meldedatum': 'Date'}, inplace=True)
            df['Region'] = 'Germany'
            region = 'Germany'

        print(df.columns)

        fig, ax = plt.subplots(figsize=figsize)
        df.loc[df['Region'] == region].plot(x='Date', y=['Cases active', 'Cases confirmed',
                                                         'Cases Recovered', 'Cases deaths'],
                                            ax=ax)
        ax.grid()
        ax.set_title(region)
