"""Module estimate switch pairs matching"""

import utilities.dataframe_operations as dfop
import pandas as pd
import numpy as np


sw_pair_match_columns = ['Fabric_name', 
                         'switchWwn_occurrence_in_switchWwn_pair', 'switchWwn_pair_occurrence_in_switchWwn', 
                         'switchWwn_pair_duplication']

def verify_switch_pair_match(switch_pair_df):
    """Function to verify switchWwn and switchWwn_pair columns match. 
    All Wwns of switchWwn column present in switchWwn_pair column and vice versa. 
    Count occurence of each column value in othjer column (switchWwn_occurence_in_switchWwn_pair, switchWwn_pair_occurence_in_switchWwn). 
    Also count occurence each Wwn in switchWwn_pair in the same column (switchWwn_pair_duplication).
    If all values are 1 then all switch pairs are matched"""
    
    switch_pair_df['switchWwn_occurrence_in_switchWwn_pair'] = switch_pair_df['switchWwn'].apply(lambda value: dfop.verify_value_occurence_in_series(value, switch_pair_df['switchWwn_pair']))
    switch_pair_df['switchWwn_pair_occurrence_in_switchWwn'] = switch_pair_df['switchWwn_pair'].apply(lambda value: dfop.verify_value_occurence_in_series(value, switch_pair_df['switchWwn']))
    switch_pair_df['switchWwn_pair_duplication'] = switch_pair_df.groupby(['switchWwn_pair'])['switchWwn'].transform('count')
    
    # for 'switchWwn_occurrence_in_switchWwn_pair', 'switchWwn_pair_occurrence_in_switchWwn' columns 
    # if switchWwn is not present then ocсurrence number is 0
    for column in sw_pair_match_columns[1:3]:
        switch_pair_df[column].fillna(0, inplace=True)  
    return switch_pair_df


def all_switch_pairs_matched(switch_pair_df):
    """Function to verify if all switch pairs matched. 
    All switchWwn values are in switchWwn_pair columns and vice versa.
    SwitchWwn_pair column has no duplicated values. 
    """
    
    return (switch_pair_df[sw_pair_match_columns[1:]] == 1).all(axis=1).all()
    

def count_switch_pairs_match_stats(switch_pair_df):
    """Function to count switch pair match statistics for each fabric name.
    Stat DataFrame has three columns: 
        ok - switchWwn quantity which are present in all trhree sw_pair_match_columns columns only once,
        absent - switchWwn quantity which are not resent in pair column (switchWwn in switchWwn_pair or vice verca),
        duplicated - switchWwn quantity which are present in pair column or in the same switchWwn_pair column more then once."""
    
    wwn_occurrence_stats_df = pd.DataFrame()
    wwn_occurrence_df = switch_pair_df[sw_pair_match_columns].copy()
    for column in sw_pair_match_columns[1:]:
        # replace 0, 1, >2 with absent, ok and duplicated. nan stays nan
        wwn_occurrence_df[column] = np.select([wwn_occurrence_df[column] == 0, wwn_occurrence_df[column] == 1, wwn_occurrence_df[column] > 1], 
                                              ['absent', 'ok', 'duplicated'], default=None)
        # count wwn occurrence stats for each fabric name
        wwn_stat_df = wwn_occurrence_df.groupby(by=['Fabric_name', column])[column].count().unstack()
        # add column name dor which stats is counted for
        wwn_stat_df['Switch_pair_stat_type'] = column
        # add current column stats to total stats DataFrame
        wwn_occurrence_stats_df = pd.concat([wwn_occurrence_stats_df, wwn_stat_df])
    # change total stats DataFrame presentation
    wwn_occurrence_stats_df.reset_index(inplace=True)
    wwn_occurrence_stats_df = dfop.move_column(wwn_occurrence_stats_df, cols_to_move='Switch_pair_stat_type', ref_col='Fabric_name')
    wwn_occurrence_stats_df.sort_values(by=['Fabric_name', 'Switch_pair_stat_type'], inplace=True)
    wwn_occurrence_stats_df.reset_index(inplace=True, drop=True)
    return wwn_occurrence_stats_df