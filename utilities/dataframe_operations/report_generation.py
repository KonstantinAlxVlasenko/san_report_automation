""" """

import pandas as pd

from .data_stucture_converting import dct_from_dataframe


def generate_report_dataframe(aggregated_df, report_headers_df, report_columns_usage_dct, *args):
    """Function to split aggregated table to required DataFrames
    As parameters function get DataFrame to be partitioned and
    list of allocated DataFrames names. Returns list of segmented DataFrames. 
    """

    # column names in report_headers_df containg header titles for each df_name
    header_names_eng_lst = [df_name + '_eng' for df_name in args]

    # import header titles from report_headers_df and drop excessive titles for each df_name
    report_header_eng_dct = {}
    for df_name, header_name_eng in zip(args, header_names_eng_lst):
        report_header_eng_dct[df_name] = header_cleanup(report_headers_df, header_name_eng, report_columns_usage_dct)

    # list with partitioned DataFrames
    report_df_lst = []
    for df_name in args:
        # identify header titles if df_name which are in aggregated_df
        df_header_eng = [column for column in report_header_eng_dct[df_name] if column in aggregated_df.columns]
        # get required columns from aggregated DataFrame
        report_df = aggregated_df.reindex(columns=df_header_eng).copy()
        # translate header to russian
        report_df = translate_header(report_df, report_headers_df, df_name)
        report_df_lst.append(report_df)
    return report_df if len(report_df_lst) == 1 else report_df_lst


def translate_header(df, headers_df, df_name):
    """Function to translate DataFrame header.
    df_name identifies columns in report_headers_df used to create translate dictionary"""

    translated_df = df.copy()
    translate_dct = dct_from_dataframe(headers_df, df_name + '_eng', df_name + '_ru')
    translated_df.rename(columns=translate_dct, inplace=True)
    return translated_df


def translate_values(df, headers_df=None, df_name=None, translated_columns=None, translate_dct={'Yes': 'Да', 'No': 'Нет', 'All': 'Итого:'}):
    """Function to translate values in corresponding columns"""

    translated_df = df.copy()

    if isinstance(headers_df, pd.DataFrame) and df_name:
        translate_dct = dct_from_dataframe(headers_df, df_name + '_eng', df_name + '_ru')

    if isinstance(translated_columns, str):
       translated_columns = [translated_columns] 
    if not translated_columns:
        translated_columns = translated_df.columns
    # columns which values need to be translated
    # translate values in column if column in DataFrame
    for column in translated_columns:
        if column in translated_df.columns:
            translated_df[column] = translated_df[column].replace(to_replace=translate_dct)
    return translated_df


def translate_dataframe(df, headers_df, df_name, translated_columns=None):
    """Function to translate DataFrame header and values in column"""
    
    translated_df = translate_values(df, headers_df, df_name, translated_columns)
    translated_df = translate_header(translated_df, headers_df, df_name)
    return translated_df


def header_cleanup(report_headers_df, header_name: str, report_columns_usage_dct) -> list:
    """Function to get DataFrame header from report_headers_df and drop excessive columns
    if they are not required"""

    column_usage_flags = [
        ('chassis_info_usage', ['chassis_name', 'chassis_wwn']),
        ('fabric_name_usage', ['Fabric_name', 'Storage_Port_Partner_Fabric_name', 'zonemember_Fabric_name']),
        ('group_name_usage', ['Group_Name'])
        ]

    if header_name not in report_headers_df.columns:
        print('\n')
        print(f'{header_name} column is MISSING')
        exit()

    header_sr = report_headers_df[header_name].dropna()

    # verify if any header titles need to be dropped
    dropped_columns = []
    for usage_flag, column in column_usage_flags:
        if not report_columns_usage_dct.get(usage_flag):
            dropped_columns.extend(column)
    if dropped_columns:
        mask_dropped_columns = header_sr.isin(dropped_columns)
        header_sr = header_sr.loc[~mask_dropped_columns]
    return header_sr.tolist()


def statistics_report(statistics_df, report_headers_df, df_name, report_columns_usage_dct, drop_columns=None):
    """Function to create report table out of statistics_df DataFrame"""

    statistics_report_df = pd.DataFrame()
    if not drop_columns:
        drop_columns = []
    if not statistics_df.empty:
        chassis_column_usage = report_columns_usage_dct.get('chassis_info_usage')
        statistics_report_df = statistics_df.copy()
        # identify columns to drop and drop columns
        if not chassis_column_usage:
            drop_columns.append('chassis_name')
        drop_columns = [column for column in drop_columns if column in statistics_df.columns]
        statistics_report_df.drop(columns=drop_columns, inplace=True)
        statistics_report_df = translate_dataframe(statistics_report_df, report_headers_df, df_name)
        # drop empty columns
        statistics_report_df.dropna(axis=1, how='all', inplace=True)
    return statistics_report_df