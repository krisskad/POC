import ast
import datetime
import glob
import json
import os
import re
import time
import zipfile

import pandas as pd
from django_pandas.io import read_frame
from MAIN.settings import BASE_DIR
import numpy as np

from collections import deque


def get_main_df(category, retailer):
    path = os.path.join(BASE_DIR, "poc_app", "demo", "{0}_{1}_dashboard.csv".format(category, retailer))

    df = pd.read_csv(path)
    # get the list of all integer columns
    int_cols = list(df.select_dtypes('int64').columns)

    # get the list of all float columns
    float_cols = list(df.select_dtypes('float64').columns)

    # get the list of all object columns
    object_cols = list(df.select_dtypes('object').columns)

    # looping through if each column to fillna
    for i in int_cols:
        df[i].fillna(0, inplace=True)

    for f in float_cols:
        df[f].fillna(0, inplace=True)

    for o in object_cols:
        df[o].fillna("", inplace=True)

    # all_files = glob.glob(path)

    # li = []
    #
    # for filename in all_files:
    #     df = pd.read_csv(filename, index_col=None, header=0)
    #     li.append(df)
    #
    # frame = pd.concat(li, axis=0, ignore_index=True)

    return df


def get_attributes(category, retailer):
    path = os.path.join(BASE_DIR, "poc_app", "demo", "autozone_attributes(1).xlsx")
    category, retailer = category.lower(), retailer.lower()
    df = pd.read_excel(path)
    df['category']= df['category'].str.lower()
    df['retailer']= df['retailer'].str.lower()
    df['attribute']= df['attribute'].str.lower()
    attr = df[(df.category == category) & (df.retailer == retailer)]["attribute"]
    # print(attr)
    return attr

''' Market Share '''


def get_marketshare(df_time_series, product_master):
    df_time_series['list_price'] = df_time_series['list_price'].fillna(df_time_series['selling_price'])
    df_time_series['revenue'] = df_time_series['delta_units_sold'] * df_time_series['selling_price']

    df_ms = df_time_series.groupby('skuid').agg({
        'list_price': 'median',
        'selling_price': 'mean',
        'delta_units_sold': 'sum',
        'revenue': 'sum'}).rename(columns={'delta_units_sold': 'units_sold'})

    df_ms['selling_price'] = (df_ms['revenue'] / df_ms['units_sold']).fillna(df_ms['selling_price'])
    df_ms['market_share'] = df_ms['revenue'] / df_ms['revenue'].sum()
    df_ms['market_share'] = df_ms['market_share'] * 100

    return product_master.join(df_ms, on='skuid', lsuffix='.1', how='inner')


def generate_pivote_whitespace(x_attr, y_attr, df):
    # print(time_series)
    # print(time_series, productmaster)
    # time_series = read_frame(time_series,
    #                          ['master_id__skuid', 'master_id', 'date_time', 'stock', 'list_price', 'selling_price',
    #                           'delta_units_sold', 'count_of_ratings', 'count_of_reviews'])
    # time_series = time_series.rename(columns={"master_id__skuid": "skuid"})
    #
    # productmaster = read_frame(productmaster, ['skuid', 'category__name', 'title', 'brand__name', 'url',
    #                                            'retailer_client__client__manufacturer',
    #                                            'attribute_data__attr_id__name', 'attribute_data__master_id__master_id',
    #                                            'attribute_data__value'])
    # productmaster = productmaster.rename(columns={"category__name": "category", "brand__name": "brand",
    #                                               'retailer_client__client__manufacturer': 'manufacturer',
    #                                               'attribute_data__attr_id__name': 'attribute_name',
    #                                               'attribute_data__master_id__master_id': 'master_id',
    #                                               'attribute_data__value': 'attribute_value'})

    # print(time_series, productmaster.columns.tolist())
    # df = get_marketshare(time_series, productmaster)
    # df = pd.read_excel("pepsi/IndusValleyNew1.xlsx")
    # print(df)
    # print(df.columns.tolist())
    # df = df.fillna('')

    # df['attribute_value'] = df['attribute_value'].str.rstrip()
    # df['attribute_value'] = df['attribute_value'].str.lstrip()

    # df.drop('Size', inplace=True, axis=1)

    # df.columns = df.columns.str.lower()
    # df.to_csv("market_share.csv", encoding='utf-8')

    report_data = df.to_dict(orient='records')

    # Convert values to columns x,y attr

    # df = (df.set_index(['skuid', 'brand', 'market_share'])
    #       .pivot(columns='attribute_name')['attribute_value']
    #       .reset_index())

    # print(df)
    # print(df.columns.tolist())
    df.columns = df.columns.str.lower()
    # print(df)

    x_attr, y_attr = x_attr.lower(), y_attr.lower()

    func = lambda x: (100 * x.sum()) / df['market_share'].sum()
    pivotTable = pd.pivot_table(df, values='market_share', index=['brand', x_attr, y_attr],
                                aggfunc=func)
    aggregated_df = pd.DataFrame(pivotTable.to_records())
    pivot_data = aggregated_df.to_dict(orient='records')
    # print(pivot_data)

    # sumOfEachTier_df = aggregated_df.groupby([y_attr, x_attr]).sum()['market_share'].to_frame(
    #     name='sum_of_each_tier').reset_index()
    # maxOfEachTier_df = aggregated_df.groupby([y_attr, x_attr]).max()['market_share'].to_frame(
    #     name='maxOfEachTier').reset_index()
    # brand_lis = []
    # for ii in maxOfEachTier_df.iterrows():
    #     brand_lis.append(aggregated_df[(aggregated_df[y_attr] == ii[1][y_attr]) & (
    #                 aggregated_df[x_attr] == ii[1][x_attr]) & (
    #                                                aggregated_df['market share'] == ii[1]['maxOfEachTier'])][
    #                          'brand'].values[0])
    # sumOfEachTier_df['brand'] = brand_lis
    # maxOfEachTier_df = sumOfEachTier_df
    #
    # pivot_data = maxOfEachTier_df.to_dict(orient='records')
    #
    # range_list = list(aggregated_df[y_attr].unique())
    # print(report_data)
    context = {"report_data": report_data,
               "pivot_data": pivot_data}

    print(context)

    return context
