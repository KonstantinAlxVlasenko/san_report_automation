import re

import numpy as np
import pandas as pd

from common_operations_dataframe import dataframe_fillna, сoncatenate_columns
from common_operations_dataframe_presentation import translate_dataframe


def verify_lic(df, lic_column: str, lic_name: str):
    """Function to check if lic_name is present on both connected switches (trunk capability)"""
    
    connected_lic_column = 'Connected_' + lic_column
    lic_columns_dct = {lic_column : lic_name + '_license', connected_lic_column : 'Connected_' + lic_name + '_license'}

    # verify lic installed on each switch 
    for licenses_column, verified_lic_column in lic_columns_dct.items():
        df[verified_lic_column] = \
            df.loc[df[licenses_column].notnull(), licenses_column].apply(lambda x: lic_name.lower() in x.lower())
        df[verified_lic_column].replace(to_replace={True: 'Yes', False: 'No'}, inplace = True)
        
    # verify lic installed on both switches
    lic_both_switches_column = lic_name + '_lic_both_switches'    
    df[lic_both_switches_column] = np.nan
    # masks license info present and license installed or not
    mask_lic_notna_both_sw = df[lic_columns_dct.values()].notna().all(axis=1)
    mask_all_yes = (df[lic_columns_dct.values()] == 'Yes').all(axis=1)
    mask_any_no = (df[lic_columns_dct.values()] == 'No').any(axis=1)
    
    df[lic_both_switches_column] = np.select(
        [mask_lic_notna_both_sw & mask_all_yes, mask_lic_notna_both_sw & mask_any_no],
        ['Yes', 'No'], default='Unknown')
    
    return df


def verify_max_link_speed(df):
    """
    Function to evaluate maximum available port speed
    and check if link operates on maximum speed.
    Maximum available link speed is calculated as minimum of next values 
    speed_chassis1, speed_chassis2, max_sfp_speed_switch1, max_sfp_speed_switch2
    """

    # columns to check speed
    speed_lst = ['Transceiver_speedMax', 'Connected_Transceiver_speedMax', 
                 'switch_speedMax', 'Connected_switch_speedMax']
    
    if pd.Series(speed_lst).isin(df.columns).all():
        # minimum of four speed columns
        mask_speed_notna = df[speed_lst].notna().all(axis=1)
        # minimum of four speed columns
        df.loc[mask_speed_notna, 'Link_speedMax'] = df.loc[mask_speed_notna, speed_lst].min(axis = 1, numeric_only=True)
        # actual link speed
        df['Link_speedActual'] = df['speed'].str.extract(r'(\d+)').astype('float64')
        # mask to check speed in columns are not None values
        mask_speed = df[['Link_speedActual', 'Link_speedMax']].notna().all(1)
        # compare values in Actual and Maximum speed columns
        df.loc[mask_speed, 'Link_speedActualMax']  = \
            pd.Series(np.where(df['Link_speedActual'].eq(df['Link_speedMax']), 'Yes', 'No'))  
    else:
        df['Link_speedMax'] = np.nan
    return df


def count_bandwidth(df, speed_column: str, connection_grp_columns: list):
    """Function to count total bandwidth between each pair of switches 
    defined by connection_grp_columns"""
    
    # extract speed values
    df['Bandwidth_Gbps'] = df[speed_column].str.extract(r'(\d+)')
    df['Bandwidth_Gbps'] = df['Bandwidth_Gbps'].astype('int64', errors='ignore')
    # group links so all links between two switches are in one group
    # count total bandwidth for each group (connection)
    bandwidth_df = df.groupby(by=connection_grp_columns, dropna=False)['Bandwidth_Gbps'].sum()
    return bandwidth_df


def count_statistics(df, connection_grp_columns: list, stat_columns: list, port_qunatity_column: str, speed_column: str):
    """Function to count statistics for each pair of switches connection.
    stat_columns is the list of columns for whish statistics is counted for,
    speed_column - column name containing link speed connecion to count
    connection bandwidth. connection_grp_columns is the list of columns defining 
    individual connection to count statistics and bandwidth for that connection."""

    statistics_df = pd.DataFrame()
    bandwidth_df = count_bandwidth(df, speed_column, connection_grp_columns)
    
    # drop empty columns from the list
    stat_columns = [column for column in stat_columns if df[column].notna().any()]
    # index list to groupby switches connection on to count statistics
    index_lst = [df[column] for column in connection_grp_columns]
    # count statistcics for each column from stat_columns in df DataFrame
    for column in stat_columns:
        # count statistics for current column
        current_statistics_df = pd.crosstab(index = index_lst,
                                columns = df[column])
        # add connection bandwidth column after column with port quantity 
        if column == port_qunatity_column:
            current_statistics_df = current_statistics_df.merge(bandwidth_df, how='left',
                                                                left_index=True, right_index=True)
        # add current_statistics_df DataFrame to statistics_df DataFrame
        if statistics_df.empty:
            statistics_df = current_statistics_df.copy()
        else:
            statistics_df = statistics_df.merge(current_statistics_df, how='left', 
                                                left_index=True, right_index=True)
    statistics_df.reset_index(inplace=True)
    return statistics_df

# count_total from datframe_operations
def count_summary(df, group_columns: list, count_columns: list=None, fn: str='sum'):
    """Function to count total for DataFrame groups. Group columns reduced by one column from the end 
    on each iteration. Count columns defines column names for which total need to be calculated.
    Function in string representation defines aggregation function to find summary values"""

    if not count_columns:
        count_columns = df.columns.tolist()
    elif isinstance(count_columns, str):
            count_columns = [count_columns]
    
    summary_df = pd.DataFrame()
    for _ in range(len(group_columns)):
        current_df = df.groupby(by=group_columns)[count_columns].agg(fn)
        current_df.reset_index(inplace=True)
        if summary_df.empty:
            summary_df = current_df.copy()
        else:
            summary_df = pd.concat([summary_df, current_df])
        # increase group size
        group_columns.pop()
    return summary_df


def count_all_row(statistics_summary_df):
    """Function to count row with index All containing total values of statistics_summary_df
    for all fabrics"""
    
    # extract row containing total values for Fabric_name
    mask_empty_fabric_label = statistics_summary_df['Fabric_label'].isna()
    statistics_total_df = statistics_summary_df.loc[mask_empty_fabric_label].copy()
    # sum values
    statistics_total_df.loc['All']= statistics_total_df.sum(numeric_only=True, axis=0)
    # rename Fabric_name to All
    statistics_total_df.loc['All', 'Fabric_name'] = 'All'
    
    # drop all rows except 'All'
    mask_fabric_name_all = statistics_total_df['Fabric_name'] == 'All'
    statistics_total_df = statistics_total_df.loc[mask_fabric_name_all].copy()
    
    statistics_total_df.reset_index(inplace=True, drop=True)
    return statistics_total_df


def concat_statistics(statistics_df, summary_df, total_df, sort_columns):
    """Function to concatenate statistics DataFrames. 
    statistics_df - statistics for each connection,
    summary_df statistics for fabric_name, fabric_label and fabric_name,
    total_df - total statistics for All fabrics.
    sort_columns used to sort concatenated statistics_df and summary_df
    to place summary statistics after corresponding fabric rows of statistics_df.
    """
    
    # concatenate statistics dataframes
    statistics_df = pd.concat([statistics_df, summary_df])
    statistics_df.sort_values(by=sort_columns, inplace=True)
    statistics_df = pd.concat([statistics_df, total_df])
    # reset indexes in final statistics DataFrame
    statistics_df.reset_index(inplace=True, drop=True)
    return statistics_df


def verify_connection_symmetry(statistics_summary_df, connection_symmetry_columns, summary_column='Asymmetry_note'):
    """Function to verify if connections are symmetric in each Fabrics_name from values in
    connection_symmetry_columns point of view. Function adds Assysmetric_note to statistics_summary_df.
    Column contains parameter name(s) for which connection symmetry condition is not fullfilled"""

    # drop invalid fabric labels
    mask_not_valid = statistics_summary_df['Fabric_label'].isin(['x', '-'])
    # drop fabric summary rows (rows with empty Fabric_label)
    mask_fabric_label_notna = statistics_summary_df['Fabric_label'].notna()
    statistics_summary_cp_df = statistics_summary_df.loc[~mask_not_valid & mask_fabric_label_notna].copy()
    
    # find number of unique values in connection_symmetry_columns
    connection_symmetry_df = \
        statistics_summary_cp_df.groupby(by='Fabric_name')[connection_symmetry_columns].agg('nunique')

    # temporary ineqaulity_notes columns for  connection_symmetry_columns
    connection_symmetry_notes = [column + '_ineqaulity' for column in connection_symmetry_columns]
    for column, column_note in zip(connection_symmetry_columns, connection_symmetry_notes):
        connection_symmetry_df[column_note] = np.nan
        # if fabrics are symmetric then number of unique values in groups should be equal to one 
        # mask_values_nonuniformity = connection_symmetry_df[column] == 1
        mask_values_nonuniformity = connection_symmetry_df[column].isin([0, 1])
        # use current column name as value in column_note for rows where number of unique values exceeds one 
        connection_symmetry_df[column_note].where(mask_values_nonuniformity, column.lower(), inplace=True)
        
    # merge temporary ineqaulity_notes columns to Asymmetry_note column and drop temporary columns
    connection_symmetry_df = сoncatenate_columns(connection_symmetry_df, summary_column, 
                                                 merge_columns=connection_symmetry_notes)
    # drop columns with quantity of unique values
    connection_symmetry_df.drop(columns=connection_symmetry_columns, inplace=True)
    # add Asymmetry_note column to statistics_summary_df
    statistics_summary_df = statistics_summary_df.merge(connection_symmetry_df, how='left', on=['Fabric_name'])
    # clean notes for dropped fabrics
    statistics_summary_df.loc[mask_not_valid, summary_column] = np.nan

    return statistics_summary_df


def summarize_statistics(statistics_df, count_columns, connection_symmetry_columns, sort_columns):
    """Function to summarize statistics by adding values in fabric_name and fabric_label, fabric_name,
    all fabrics"""

    count_columns = [column for column in statistics_df.columns if statistics_df[column].notna().any()]
    # summary_statistics for fabric_name and fabric_label, fabric_name
    statistics_summary_df = \
        count_summary(statistics_df, group_columns=['Fabric_name', 'Fabric_label'], count_columns=count_columns, fn=sum)
    # verify if fabrics are symmetrical from connection_symmetry_columns point of view
    statistics_summary_df = \
        verify_connection_symmetry(statistics_summary_df, connection_symmetry_columns)
    # total statistics for all fabrics
    statistics_total_df = count_all_row(statistics_summary_df)
    # concatenate all statistics in certain order
    statistics_df = concat_statistics(statistics_df, statistics_summary_df, statistics_total_df, sort_columns)
    return statistics_df


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


def  convert_wwn(df, wwn_columns: list):
    """Function to convert Wwnn and Wwnp to regular represenatation (lower case with colon delimeter)"""

    for wwn_column in wwn_columns:
        if wwn_column in df.columns and df[wwn_column].notna().any():
            mask_wwn = df[wwn_column].notna()
            df.loc[mask_wwn, wwn_column] = df.loc[mask_wwn, wwn_column].apply(lambda wwn: ':'.join(re.findall('..', wwn)))
            df[wwn_column] = df[wwn_column].str.lower()
    return df


def replace_wwnn(wwn_df, wwn_column: str, wwnn_wwnp_df, wwnn_wwnp_columns: list, fabric_columns: list=[]):
    """Function to replace wwnn in wwn_column (column with presumably mixed wwnn and wwnp values) 
    of wwn_df DataFrame with corresponding wwnp value if wwnn is present. wwnn_wwnp_df DataFrame contains strictly defined 
    wwnn and wwnp values in corresponding columns which passed as wwnn_wwnp_columns parameter.
    fabric_columns contains additional columns if required find wwnp for wwnn in certain fabric only."""
    
    wwnn_column, wwnp_column = wwnn_wwnp_columns
    join_columns = [*fabric_columns, wwnn_column]

    if wwnp_column in wwn_df.columns:
        wwn_df[wwnp_column] = np.nan

    # assume that all values in wwn_column are wwnns
    wwn_df[wwnn_column] = wwn_df[wwn_column]
    # find corresponding wwnp value from wwnn_wwnp_df for each presumed wwnn in wwn_df
    # rows with filled values in wwnp_column have confirmed wwnn value in  wwnn_column column of wwn_df
    wwn_df = dataframe_fillna(wwn_df, wwnn_wwnp_df, 
                                    join_lst=join_columns, 
                                    filled_lst=[wwnp_column], remove_duplicates=False)
    # when rows have empty values in wwnp_column mean wwn doesn't exist in fabric or it is wwnp
    wwn_df[wwnp_column].fillna(wwn_df[wwn_column], inplace=True)
    wwn_df.drop(columns=[wwnn_column], inplace=True)
    return wwn_df


def tag_value_in_column(df, column, tag, binding_char='_'):
    """Function to tag all notna values in DataFrame column with tag"""

    tmp_column = 'tag_column'
    # change temp column name if column in DataFrame
    while tmp_column in df.columns:
        tmp_column += '_'

    mask_value_notna = df[column].notna()
    df[tmp_column] = tag + binding_char
    df[column] = df[column].astype('Int64', errors='ignore').astype('str', errors='ignore')
    df[column] = df.loc[mask_value_notna, tmp_column] + df.loc[mask_value_notna, column]
    df.drop(columns=tmp_column, inplace=True)

    return df