"""Module with functions to update paired switch names, assign switch pairs ids after switch_apir_df change"""

import pandas as pd
import numpy as np
import utilities.dataframe_operations as dfop
from .switch_pair_verification import verify_switch_pair_match


def assign_switch_pair_id(switch_pair_df):
    """Function to assighn switch pair Id based on sorted combination of switchWwn and switchWwn_paired"""
    
    # switch_pair_df.reset_index(drop=True, inplace=True)
    # merge wwns of the paired swithes
    switch_pair_df = dfop.merge_columns(switch_pair_df, summary_column='switchPair_wwns', merge_columns=['switchWwn', 'switchWwn_pair'], drop_merge_columns=False)
    # sort merged wwns in cells to have identical cell values for both of the paired rows
    dfop.sort_cell_values(switch_pair_df, 'switchPair_wwns')
    # numbering identical switch_pair_wwns
    switch_pair_df['switchPair_id'] = switch_pair_df.groupby(['switchPair_wwns']).ngroup()
    switch_pair_df['switchPair_id'] = switch_pair_df['switchPair_id'] + 1
    switch_pair_df.sort_values(by=['Fabric_name', 'switchPair_id'], inplace=True)
    return switch_pair_df


def update_switch_pair_dataframe(switch_pair_df):
    """Function to update switchName and switchWwn occurrence columns after manual 
    switchWwn_pair correction in switch_pair_df change"""
    
    # correct switch names
    sw_wwn_name_match_sr = create_wwn_name_match_series(switch_pair_df)
    switch_pair_df['switchName_pair'] = switch_pair_df.apply(lambda series: switch_name_correction(series, sw_wwn_name_match_sr), axis=1)
    # correct switchWwn occurence
    switch_pair_df = verify_switch_pair_match(switch_pair_df)
    return switch_pair_df


def switch_name_correction(series, sw_wwn_name_match_sr):
    """Function to correct switchName of the paired switch after manual switchWwn_pair correction"""
    
    if pd.isna(series['switchWwn_pair']):
        return np.nan
    
    sw_name_lst = [sw_wwn_name_match_sr[wwn] for wwn in series['switchWwn_pair'].split(', ')]
    if sw_name_lst:
        return ', '.join(sw_name_lst)


def create_wwn_name_match_series(switch_pair_df):
    """Function to create series containing switchWwn to switchName match"""
    
    sw_wwn_name_match_sr = dfop.series_from_dataframe(switch_pair_df.drop_duplicates(subset=['switchWwn']), 
                                                      index_column='switchWwn', value_column='switchName')
    return sw_wwn_name_match_sr