"""Module to count switch statistics in SAN"""


import numpy as np
import pandas as pd

from common_operations_dataframe import count_frequency
from common_operations_switch import count_all_row, count_summary

def fabric_switch_statistics(switch_params_aggregated_df, re_pattern_lst):
    """Function to count switch statistics"""

    # modify switch_params_aggregated_df to count statistics
    switch_params_cp_df = prior_prepearation(switch_params_aggregated_df, re_pattern_lst)
    # count switch statistics (logical and physical) in each fabric_name, fabric_label
    fabric_switch_statistics_df = count_switch_statistics(switch_params_cp_df)
    # count chassis statistics (physical only) for each fabric_label
    san_chassis_statistics_df = count_chassis_statistics(switch_params_cp_df)
    # concatenate switch and chassis statistics
    fabric_switch_statistics_df = pd.concat([fabric_switch_statistics_df, san_chassis_statistics_df], ignore_index=True)
    return fabric_switch_statistics_df
    

def prior_prepearation(switch_params_aggregated_df, re_pattern_lst):
    """Function to modify switch_params_aggregated_df to count statistics"""

        # regular expression patterns
    *_, comp_dct = re_pattern_lst
    
    switch_params_cp_df = switch_params_aggregated_df.copy()
    # remove uninfomative values from switch DataFrame
    maps_clean_pattern = comp_dct['maps_clean']
    switch_params_cp_df.replace(to_replace={maps_clean_pattern: np.nan} , regex=True, inplace=True)
    # change count columns values representation
    switch_params_cp_df['LS_type'] = switch_params_cp_df['LS_type'].str.capitalize() + '_sw'

    mask_router = switch_params_cp_df['FC_Router'] == 'ON'
    switch_params_cp_df.loc[mask_router, 'FC_Router_ON'] = 'FC_Router'

    mask_fv_lic = switch_params_cp_df['Fabric_Vision_license'].isin(['Yes', 'Да'])
    mask_trunking_lic = switch_params_cp_df['Trunking_license'].isin(['Yes', 'Да'])

    switch_params_cp_df.loc[mask_fv_lic, 'Fabric_Vision_lic'] = 'Fabric_Vision_lic'
    switch_params_cp_df.loc[mask_trunking_lic, 'Trunking_lic'] = 'Trunking_lic'

    switch_params_cp_df['Total'] = 'Total'
    return switch_params_cp_df


def count_switch_statistics(switch_params_cp_df):
    """Function to count switch statistics (logical and physical) in fabric_name and fabric_label,
    fabric_name, total san levels"""

    # count values for fabric_name and fabric_label level
    count_columns = ['Total', 'ModelName', 'Generation', 'Trunking_lic', 'Fabric_Vision_lic', 
                    'FC_Router_ON', 'LS_type', 'SwitchMode', 'Current_Switch_Policy_Status']

    fabric_switch_statistics_df = count_frequency(switch_params_cp_df, count_columns, 
                                                group_columns=['Fabric_name', 'Fabric_label'],
                                                margin_column_row=(False, False))
    # count values for fabric_name level by default for all columns
    fabric_switch_statistics_df = count_summary(fabric_switch_statistics_df, group_columns=['Fabric_name', 'Fabric_label'])
    fabric_switch_statistics_df.sort_values(by=['Fabric_name', 'Fabric_label'], inplace=True)
    # count values for san level
    fabric_switch_statistics_total_df = count_all_row(fabric_switch_statistics_df)
    fabric_switch_statistics_total_df['Fabric_name'] = 'Total switches'
    
    fabric_switch_statistics_df = pd.concat([fabric_switch_statistics_df, fabric_switch_statistics_total_df], ignore_index=True)
    return fabric_switch_statistics_df


def count_chassis_statistics(switch_params_cp_df):
    """Function to count chassis statistics (physical only) for fabric_label, total san levels"""

    # filter unique chassis only across all fabrics
    chassis_columns = ['configname', 'chassis_name', 'chassis_wwn']
    chassis_cp_df = switch_params_cp_df.drop_duplicates(subset=chassis_columns).copy()
    chassis_cp_df['Fabric_name'] = 'Total chassis'
    # count values for fabric_label level
    chassis_stat_columns = ['Total', 'ModelName', 'Generation', 'Trunking_lic', 'Fabric_Vision_lic']
    san_chassis_statistics_df = count_frequency(chassis_cp_df, chassis_stat_columns, 
                                                group_columns=['Fabric_name', 'Fabric_label'],
                                                margin_column_row=(False, False))
    # count values for san level
    san_chassis_statistics_df = count_summary(san_chassis_statistics_df, group_columns=['Fabric_name', 'Fabric_label'])
    return san_chassis_statistics_df
