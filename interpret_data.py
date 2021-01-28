#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
import numpy as np
import re
import time
from datetime import datetime


DATASTORE = './data/'
DATAFILE = 'projectsearch-1611840195995.csv'
OUTPUT = './output/'

def import_csv_to_df(location, filename):
    """
    Imports a csv file into a Pandas dataframe
    :params: an csv file and a filename from that file
    :return: a df
    """

    return pd.read_csv(location + filename)


def export_to_csv(df, location, filename, index_write):
    """
    Exports a df to a csv file
    :params: a df and a location in which to save it
    :return: nothing, saves a csv
    """

    return df.to_csv(location + filename + '.csv', index=index_write)


def convert_to_date(df):
    df['StartDate'] = pd.to_datetime(df['StartDate'])
    df['EndDate'] = pd.to_datetime(df['StartDate'])
    return df


def limit_to_funder(df, funder):

    df = df[df['FundingOrgName'] == funder]
    if len(df) == 0:
        print('Probably no funder with that name')

    return df


def limit_to_institution(df, institution):

    df = df[df['LeadROName']==institution]
    if len(df) == 0:
        print('Probably no institution with that name')

    return df


def limit_to_date(df, datestart, dateend):

    if datestart != False:
        df = df[df['StartDate'].dt.year >= datestart]

    if dateend != False:
        df = df[df['EndDate'].dt.year <= dateend]

    return df

def sort_df(df):

    df.sort_values(by=['AwardPounds'], ascending=False, inplace=True, na_position='last')

    return df


def main():
    """
    Main function to run program
    """

    funder = 'AHRC'
    datestart = False
    dateend = False
    institution = False
    sort_by_award = True

    df = import_csv_to_df(DATASTORE, DATAFILE)

    df = convert_to_date(df)

    if funder!=False:
        df = limit_to_funder(df, funder)

    if datestart!=False or dateend!=False:
        df = limit_to_date(df, datestart, dateend)

    if institution != False:
        df = limit_to_institution(df, institution)

    if sort_by_award != False:
        df = sort_df(df)

    print(df)

    export_to_csv(df, OUTPUT, 'GtR_analysed', False)

if __name__ == '__main__':
    main()
