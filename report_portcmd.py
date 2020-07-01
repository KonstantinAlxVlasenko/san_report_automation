"""
Module to create tables 
- list of servers, storages, libraries
- list of hbafw and drivers
- list of servers, storages, libraries san connection 
"""

import pandas as pd
from common_operations_dataframe import dataframe_segmentation


def create_report_tables(portshow_aggregated_df, data_names, report_columns_usage_dct, max_title):
    """Function to create required report DataFrames out of aggregated DataFrame"""

    add_columns_lst = ['FW_Recommeneded', 'Driver_Recommeneded', 'FW_Supported', 'HW_Supported']
    portshow_aggregated_df = portshow_aggregated_df.reindex(columns=[*portshow_aggregated_df.columns.tolist(), *add_columns_lst])
    # partition aggregated DataFrame to required tables
    # pylint: disable=unbalanced-tuple-unpacking
    servers_report_df, storage_report_df, library_report_df, hba_report_df, \
        storage_connection_df,  library_connection_df, server_connection_df, npiv_report_df = \
        dataframe_segmentation(portshow_aggregated_df, data_names, report_columns_usage_dct, max_title)
    
    # clean and sort DataFrames
    servers_report_df = _clean_dataframe(servers_report_df, 'srv')
    hba_report_df = _clean_dataframe(hba_report_df, 'srv', duplicates = ['Идентификатор порта WWPN'])
    storage_report_df = _clean_dataframe(storage_report_df, 'stor')
    storage_report_df = _multi_fabric(storage_report_df, report_columns_usage_dct)
    library_report_df = _clean_dataframe(library_report_df, 'lib')
    storage_connection_df = _clean_dataframe(storage_connection_df, 'stor', clean = True)

    library_connection_df = _clean_dataframe(library_connection_df, 'lib', clean = True)
    server_connection_df = _clean_dataframe(server_connection_df, 'srv', clean = True)
    npiv_report_df = _clean_dataframe(npiv_report_df, 'npiv', duplicates = None, clean = True)

    return servers_report_df, storage_report_df, library_report_df, \
        hba_report_df, storage_connection_df,  library_connection_df, server_connection_df, npiv_report_df
    

def _clean_dataframe(df, mask_type, duplicates = ['Фабрика', 'Имя устройства', 'Имя группы псевдонимов'], clean = False):
    """
    Auxiliary function to sort, remove duplicates and drop columns in cases they are not required in report DataFrame
    """
    # list of columns to check if they are empty
    columns_empty = ['Медленное устройство', 'Подключено через AG', 'Real_device_behind_AG']
    # list of columns to check if all values are equal
    columns_unique = ['Режим коммутатора', 'LSAN', 'Connected_NPIV']
    # list of columns to sort DataFrame on
    columns_sort = [
        'Фабрика', 'Расположение', 'Имя устройства', 'Порт устройства', 
        'Подсеть', 'Имя коммутатора', 'Идентификатор порта WWPN'
        ]

    # create mask to filter required class only
    if mask_type == 'srv':
        mask = df['Класс устройства'].isin(['SRV', 'BLADE_SRV'])
    elif mask_type == 'stor':
        mask = df['Класс устройства'] == 'STORAGE'
    elif mask_type == 'lib':
        mask = df['Класс устройства'] == 'LIB'
    elif mask_type == 'npiv':
        mask = df['Connected_NPIV'] == 'yes'

    # filter DataFrame base on hardware type 
    df = df.loc[mask].copy()
    # check if columns required to sort on are in the DataFrame
    columns_sort = [column for column in columns_sort if column in df.columns]
    df.sort_values(by = columns_sort, inplace = True)

    if duplicates:
        duplicates = [column for column in duplicates if column in df.columns]

    # DataFrames are cleaned in two ways
    # by drop duplicate values in certain columns
    if duplicates and not clean:
        df.drop_duplicates(subset = duplicates, inplace = True)
    # or by drop entire column
    if clean:
        # if all values in the column are the same
        for column in columns_unique:
            if column in df.columns and df[column].nunique() < 2:
                df.drop(columns = [column], inplace = True)
        # if all values are None in the column
        for column in columns_empty:
            # if all values are None
            if column in df.columns and pd.isnull(df[column]).all():
                df.drop(columns = [column], inplace = True)
            # if all non None values are 'No'
            elif column in df.columns and pd.Series(df[column] == 'No', pd.notna(df[column])).all():
                df.drop(columns = [column], inplace = True)

    return df


def _multi_fabric(df, report_columns_usage_dct):
    """
    Function to check if device ports connected to different Fabrics.
    For example, main Fabric and replication Fabric.
    If yes then join Fabric cells values in one cell
    """

    df_columns = df.columns

    if not 'Имя устройства' in df_columns:
        return df

    if report_columns_usage_dct['fabric_name_usage'] and not df['Имя устройства'].is_unique:
        # if severeal aliases for one wwnp then combine all into one alias
        identical_values = {k: 'first' for k in df.columns[2:]}
        df = df.groupby(['Имя устройства'], as_index = False).agg({**{'Фабрика': ', '.join}, **identical_values})
        df = df.reindex(columns = df_columns)

    return df


