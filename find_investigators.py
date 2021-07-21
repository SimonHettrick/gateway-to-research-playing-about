#!/usr/bin/env python
# encoding: utf-8
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
import html5lib

DATASTORE = './output/'
DATAFILE = 'GtR_analysed.csv'
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


def limit_ten_records(df):

    # Cut to the first 10 rows to keep things simple when testing how this works out
    df=df[:2]

    return df


def convert_to_date(df):
    df['StartDate'] = pd.to_datetime(df['StartDate'])
    df['EndDate'] = pd.to_datetime(df['StartDate'])
    return df


def get_links(df):

    dict_links = dict(zip(df.ProjectReference, df.GTRProjectUrl))

    return dict_links


def get_investigators(dict_links):

    def parse_grant(url, all_investigators_df):
        r = requests.get(url)
        #print(r.content)

        current_page = BeautifulSoup(r.content, 'html5lib')
        #print(current_page.prettify())

        grant_ref = current_page.find('gtr:grantreference').text
        #investigators_section = current_page.find('gtr:personroles')

        current_grant={}
        # See Example person roles xml.txt for info on this
        for investigator_section in current_page.find('gtr:personroles'):
            current_person ={}
            id = investigator_section.find('gtr:id').text
            firstname =  investigator_section.find('gtr:firstname').text
            print(firstname)
            try:
                othernames = investigator_section.find('gtr:othernames').text
            except:
                othernames = ''
                pass
            surname = investigator_section.find('gtr:surname').text
            role = investigator_section.find('gtr:role').text

            current_person['firstname'] = firstname
            current_person['othernames'] = othernames
            current_person['surname'] = surname
            current_person['role'] = role

            current_grant[id] = current_person

            current_grant_series = pd.Series([grant_ref, id, firstname, othernames, surname, role],
                                             index=['grant ref', 'id', 'firstname', 'othernames', 'surname', 'role'])
            all_investigators_df = all_investigators_df.append(current_grant_series, ignore_index=True)


        all_investigators[grant_ref] = current_grant
        return all_investigators_df

    #url = 'https://gtr.ukri.org/projects?ref=AH/M010163/1'

    all_investigators = {}
    all_investigators_df = pd.DataFrame(columns=['grant ref', 'id', 'firstname', 'othernames', 'surname', 'role'] )

    for key in dict_links:
        all_investigators_df = parse_grant(dict_links[key], all_investigators_df)

    print(all_investigators_df)

    return all_investigators


def main():
    """
    Main function to run program
    """

    # Load up GtR data and get links to GtR projects
    df = import_csv_to_df(DATASTORE, DATAFILE)
    df = limit_ten_records(df)

    dict_links = get_links(df)

    all_investigators = get_investigators(dict_links)


if __name__ == '__main__':
    main()