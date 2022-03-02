"""Module to count switch statistics in SAN"""


import numpy as np
import pandas as pd
import utilities.dataframe_operations as dfop


def fabric_switch_statistics(switch_params_aggregated_df, pattern_dct):
    """Function to count switch statistics"""

    # modify switch_params_aggregated_df to count statistics
    switch_params_cp_df = prior_prepearation(switch_params_aggregated_df, pattern_dct)
    # count switch statistics (logical and physical) in each fabric_name, fabric_label
    fabric_switch_statistics_df = count_switch_statistics(switch_params_cp_df)
    # count chassis statistics (physical only) for each fabric_label
    san_chassis_statistics_df = count_chassis_statistics(switch_params_cp_df)
    # concatenate switch and chassis statistics
    fabric_switch_statistics_df = pd.concat([fabric_switch_statistics_df, san_chassis_statistics_df], ignore_index=True)

    fabric_switch_statistics_df = asymmetry_note(switch_params_cp_df, fabric_switch_statistics_df)

    return fabric_switch_statistics_df
    

def asymmetry_note(switch_params_cp_df, fabric_switch_statistics_df):
    """Function to verify fabric symmetry from switch model, generation and mode point of view"""

    sw_models = switch_params_cp_df['ModelName'].unique().tolist()
    sw_gen = switch_params_cp_df['Generation'].unique().tolist()
    sw_role = switch_params_cp_df['SwitchMode'].unique().tolist()

    # fabric_switch_statistics_df = dfop.verify_symmetry_regarding_fabric_name(fabric_switch_statistics_df, sw_models, summary_column='Model_Asymmetry_note')
    # fabric_switch_statistics_df = dfop.verify_symmetry_regarding_fabric_name(fabric_switch_statistics_df, sw_gen, summary_column='Generation_Asymmetry_note')
    # fabric_switch_statistics_df = dfop.verify_symmetry_regarding_fabric_name(fabric_switch_statistics_df, sw_role, summary_column='Mode_Asymmetry_note')

    fabric_switch_statistics_df = dfop.verify_group_symmetry(fabric_switch_statistics_df, symmetry_grp=['Fabric_name'], symmetry_columns=sw_models, summary_column='Model_Asymmetry_note')
    fabric_switch_statistics_df = dfop.verify_group_symmetry(fabric_switch_statistics_df, symmetry_grp=['Fabric_name'], symmetry_columns=sw_gen, summary_column='Generation_Asymmetry_note')
    fabric_switch_statistics_df = dfop.verify_group_symmetry(fabric_switch_statistics_df, symmetry_grp=['Fabric_name'], symmetry_columns=sw_role, summary_column='Mode_Asymmetry_note')


    return fabric_switch_statistics_df



def prior_prepearation(switch_params_aggregated_df, pattern_dct):
    """Function to modify switch_params_aggregated_df to count statistics"""

    
    mask_valid_fabric = ~switch_params_aggregated_df[['Fabric_name', 'Fabric_label']].isin(['x', '-']).any(axis=1)
    mask_not_vc = ~switch_params_aggregated_df['ModelName'].str.contains('virtual', case=False, na=False)
    switch_params_cp_df = switch_params_aggregated_df.loc[mask_valid_fabric & mask_not_vc].copy()
    
    # remove uninfomative values from switch DataFrame
    maps_clean_pattern = pattern_dct['maps_clean']
    switch_params_cp_df.replace(to_replace={maps_clean_pattern: np.nan} , regex=True, inplace=True)
    # change count columns values representation
    switch_params_cp_df['LS_type'] = switch_params_cp_df['LS_type'].str.capitalize() + '_sw'

    mask_router = switch_params_cp_df['FC_Router'] == 'ON'
    switch_params_cp_df.loc[mask_router, 'FC_Router_ON'] = 'FC_Router'

    mask_fv_lic = switch_params_cp_df['Fabric_Vision_license'].isin(['Yes', 'Да'])
    mask_trunking_lic = switch_params_cp_df['Trunking_license'].isin(['Yes', 'Да'])

    switch_params_cp_df.loc[mask_fv_lic, 'Fabric_Vision_lic'] = 'Fabric_Vision_lic'
    switch_params_cp_df.loc[mask_trunking_lic, 'Trunking_lic'] = 'Trunking_lic'

    switch_params_cp_df['ModelName'].fillna('Unknown_model', inplace=True)
    switch_params_cp_df['Generation'].fillna('Uknown_Gen', inplace=True)
    switch_params_cp_df['SwitchMode'].fillna('Uknown_Mode', inplace=True)

    mask_switch_pair_found = switch_params_cp_df.groupby(by=['Fabric_name', 'switchPair_id'])['switchPair_id'].transform('count') > 1
    switch_params_cp_df['switchPaired'] = 'Unpaired_switch'
    switch_params_cp_df.loc[mask_switch_pair_found, 'switchPaired'] = 'Paired_switch'

    # add config collection tag to date
    mask_date_notna = switch_params_cp_df['config_collection_date_ymd'].notna()
    switch_params_cp_df.loc[mask_date_notna, 'config_collection_date_ymd'] = 'Config date ' + switch_params_cp_df.loc[mask_date_notna, 'config_collection_date_ymd'].astype(str)

    switch_params_cp_df['Total'] = 'Total'

    return switch_params_cp_df


def count_switch_statistics(switch_params_cp_df):
    """Function to count switch statistics (logical and physical) in fabric_name and fabric_label,
    fabric_name, total san levels"""

    # count values for fabric_name and fabric_label level
    count_columns = ['Total', 'ModelName', 'Generation', 'switchPaired', 'Trunking_lic', 'Fabric_Vision_lic', 
                    'FC_Router_ON', 'LS_type', 'SwitchMode', 'Current_Switch_Policy_Status', 'config_collection_date_ymd']

    fabric_switch_statistics_df = dfop.count_frequency(switch_params_cp_df, count_columns, 
                                                group_columns=['Fabric_name', 'Fabric_label'],
                                                margin_column_row=(False, False))
    # count values for fabric_name level by default for all columns
    fabric_switch_statistics_df = dfop.count_summary(fabric_switch_statistics_df, group_columns=['Fabric_name', 'Fabric_label'])
    fabric_switch_statistics_df.sort_values(by=['Fabric_name', 'Fabric_label'], inplace=True)
    # count values for san level
    fabric_switch_statistics_total_df = dfop.count_all_row(fabric_switch_statistics_df)
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
    chassis_stat_columns = ['Total', 'ModelName', 'Generation', 'switchPaired', 'Trunking_lic', 'Fabric_Vision_lic', 'config_collection_date_ymd']
    san_chassis_statistics_df = dfop.count_frequency(chassis_cp_df, chassis_stat_columns, 
                                                group_columns=['Fabric_name', 'Fabric_label'],
                                                margin_column_row=(False, False))
    # count values for san level
    san_chassis_statistics_df = dfop.count_summary(san_chassis_statistics_df, group_columns=['Fabric_name', 'Fabric_label'])
    return san_chassis_statistics_df

