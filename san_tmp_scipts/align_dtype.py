# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 20:34:24 2023

@author: kavlasenko
"""

import pandas as pd
import numpy as np


def align_columns_object_dtype(df, source_column: str, destination_column: str):
    
    if pd.api.types.is_object_dtype(df[source_column]):
        df[destination_column] = df[destination_column].astype('object')
        

def column_fillna(df, source_column: str, destination_column: str):
    
    align_columns_object_dtype(df, source_column, destination_column)
    df[destination_column].fillna(tst_df[source_column], inplace=True)
    


tst_df = pd.DataFrame([[1,'a', np.nan], [3, np.nan, np.nan], [5, 'c', np.nan]], columns=['a', 'b', 'c'])


tst2_df = pd.DataFrame([[1,'a', np.nan], [3, np.nan, np.nan], [5, 'c', np.nan]], columns=['a', 'b', 'c'])

pd.concat([tst_df, tst2_df])


tst_df.dropna(axis=1, how='all')

tst_df['c'] = np.nan


tst_df.dtypes

tst_df['c'] = tst_df['c'].astype('object')

pd.concat([tst2_df])
        

align_columns_object_dtype(tst_df, source_column='b', destination_column='c')



tst_df['c'].fillna(tst_df['b'], inplace=True)





tst_df['c'].fillna(tst_df['a'], inplace=True)


tst_df['c'] = tst_df['c'].fillna(tst_df['b'])

tst_df['d'] = None

tst_df['d'].isna()

is_col_object_dtype = (df[column].dtype == object)



pd.api.types.is_object_dtype(tst_df['c'])


tst_df = pd.DataFrame([[1,'a'], [3, np.nan], [5, 'c'], [6, np.nan]], columns=['a', 'b'])

tst_df['b'].ffill(inplace=True)

tst_df['d'] = None
tst_df['d'].isna()

extract_pattern_columns_lst = [
    [1, ['Message_portIndex', 'Message_portType', 'slot', 'port']],
    [1, ['Message_portId']],
    [1, ['Condition', 'Message_portIndex']],
    [1, ['Condition', 'slot', 'Message_portIndex']],
    [1, ['Condition', 'slot', 'port', 'Current_value']],
    [1, ['slot', 'port', 'Condition']],
    [1, ['Condition', 'Message_portType', 'slot', 'port']],
    [1, ['Condition', 'Current_value', 'Dashboard_category']],
    [1, ['Condition', 'obj']],
    [1, ['Condition', 'slot', 'port', 'Message_portIndex', 'Message_portId']],
    [1, ['Condition','slot', 'port', 'Message_portIndex']],
    [1, ['Condition', 'tx_port', 'rx_port', 'sid', 'did']],
    [1, ['Condition', 'Message_portIndex', 'did', 'sid', 'wwn']],
    [1, ['slot', 'port']],
    [1, ['Dashboard_category', 'Condition', 'IP_Address']],
    [1, ['Dashboard_category', 'Condition', 'IP_Address']],
    ] 

extract_columns_lst = [extract_pattern_columns[1] for  extract_pattern_columns in extract_pattern_columns_lst]

def flatten(arg):
    """Function returns flat list out of list of lists"""

    if not isinstance(arg, list): # if not list
        return [arg]
    return [x for sub in arg for x in flatten(sub)]


def remove_diplicates_from_list(lst):

    return list(dict.fromkeys(lst))

extract_columns_lst = flatten(extract_columns_lst)
extract_columns_lst = remove_diplicates_from_list(extract_columns_lst)


extract_columns_lst = list(dict.fromkeys(extract_columns_lst))


    


tst_df.reindex(columns=['a', 'e'])


def concatenate_dataframes(*args):

    # concatenate all non empty DataFrames
    concatenated_df = pd.concat([df for df in args if not df.empty])
    concatenated_df.reset_index(drop=True, inplace=True)

    # get columns of all DataFrames
    dfs_columns_lst = [df.columns.to_list() for df in args]
    dfs_columns_lst = flatten(dfs_columns_lst)
    dfs_columns_lst = remove_diplicates_from_list(dfs_columns_lst)

    # add absent columns if any DataFrame from args is empty
    concatenated_df = concatenated_df.reindex(columns=dfs_columns_lst)
    
    return concatenated_df

tst3_df = concatenate_dataframes(tst_df, tst2_df)