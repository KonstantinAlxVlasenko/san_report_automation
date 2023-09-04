"""Module to count sfp modules statistics"""


import pandas as pd
import numpy as np

import utilities.dataframe_operations as dfop


def sfp_prior_preparation(portshow_sfp_aggregated_df, pattern_dct):
    """Function to preprocess portshow_sfp_aggregated_df to count statistics.
    Add comments to values, set tags to port and transceivers"""

    sfp_aggregated_modified_df =  portshow_sfp_aggregated_df.copy()
    # drop duplicated port rows
    sfp_aggregated_modified_df.drop_duplicates(subset=['configname', 'chassis_name', 'chassis_wwn', 
                                                            'switchName', 'switchWwn', 'slot', 'port'], inplace=True)
    # extract transceiver speed
    sfp_aggregated_modified_df['Transceiver_speed_extracted'] = \
        sfp_aggregated_modified_df['Transceiver_mode'].str.extract(pattern_dct['transceiver_speed'])
    # extract transceiver mode
    sfp_aggregated_modified_df['Transceiver_mode_extracted'] = \
        sfp_aggregated_modified_df['Transceiver_mode'].str.extract(pattern_dct['transceiver_mode'])
    # merge sfp speed and mode (lw, sw)
    sfp_aggregated_modified_df = dfop.merge_columns(sfp_aggregated_modified_df, summary_column='Transceiver_speed_mode_extracted', 
                                                    merge_columns=['Transceiver_speed_extracted', 'Transceiver_mode_extracted'], 
                                                    sep=' ', drop_merge_columns=False, sort_summary=False)
    # merge port state with transceiver details
    # add 'No_SFP_module' tag for cu media in blade switches to mark portPhys status
    # mask_vendor_no_sfp_module = sfp_aggregated_modified_df['Transceiver_Name'] == 'No SFP module'
    # mask_portphys_no_module = sfp_aggregated_modified_df['portPhys'] == 'No_Module'
    # sfp_aggregated_modified_df.loc[~mask_portphys_no_module & mask_vendor_no_sfp_module, 'No_SFP_module'] = 'No_SFP_module'
    sfp_aggregated_modified_df = dfop.merge_columns(sfp_aggregated_modified_df, summary_column='PortPhys_transceiver', 
                                                    merge_columns=['portPhys', 'Transceiver_speed_mode_extracted'], 
                                                    sep=' ', drop_merge_columns=False, sort_summary=False)
    # add annotation to the intervals
    comment_sfp_readings_interval(sfp_aggregated_modified_df)   
    # transceiver support
    comment_sfp_support(sfp_aggregated_modified_df)
    # transceiver form factor (qsfp, dsfp)
    comment_specific_sfp(sfp_aggregated_modified_df, sfp_specification_column='Transceiver_form_factor', sfp_specification_name='Form factor', normal_value='sfp', upper_case_spec=True)
    # long distance sfps
    comment_specific_sfp(sfp_aggregated_modified_df, sfp_specification_column='Transceiver_distanceMax', sfp_specification_name='Distance', normal_value='normal')
    # merge vendor, part number and transcever details (speed and mode)    
    sfp_aggregated_modified_df = dfop.merge_columns(
        sfp_aggregated_modified_df, summary_column='Transceiver_Name_PN', 
        merge_columns=['Transceiver_Name', 'Transceiver_PN', 'Transceiver_speed_mode_extracted'], 
        sep=' ', drop_merge_columns=False)
    # port_quantity column
    sfp_aggregated_modified_df['Port_quantity'] = 'Port_quantity'
    # transceiver quantity column
    mask_sfp_pn_notna = sfp_aggregated_modified_df['Transceiver_PN'].notna()
    sfp_aggregated_modified_df.loc[mask_sfp_pn_notna, 'Transceiver_quantity'] = 'Transceiver_quantity'
    return sfp_aggregated_modified_df


def comment_sfp_readings_interval(sfp_aggregated_modified_df):
    """Function to add column name and tranceiver mode if requested 
    to the columns containing intervals"""

    # columns with readings and sfp mode inclusion bool flag 
    readings_columns = [['RX_Power_dBm', True],
                        ['TX_Power_dBm', True],
                        ['RX_Power_uW', True],
                        ['TX_Power_uW', True],
                        ['Temperature_Centigrade', False],
                        ['Pwr_On_Time_years', False]
                        ]
    
    for readings_column, include_sfp_mode in readings_columns:
        # column with intervals without comments
        interval_readings_column = readings_column + '_interval'
        # column with commented intervals
        stat_readings_column = readings_column + '_stats'

        # not empty interval filter
        mask_interval_notna = sfp_aggregated_modified_df[interval_readings_column].notna()
        if not mask_interval_notna.any():
            sfp_aggregated_modified_df[stat_readings_column] = np.nan
            continue
        if include_sfp_mode:
            sfp_aggregated_modified_df[stat_readings_column] = readings_column + " interval " + \
                "(sfp " + sfp_aggregated_modified_df.loc[mask_interval_notna, 'Transceiver_mode_extracted'] + ") " +\
                "'" + sfp_aggregated_modified_df.loc[mask_interval_notna, interval_readings_column] + "'"
        else:
            sfp_aggregated_modified_df[stat_readings_column] = readings_column + " interval " + "'" + \
                sfp_aggregated_modified_df.loc[mask_interval_notna, interval_readings_column] + "'"


def comment_sfp_support(sfp_aggregated_modified_df):
    """Function to mark supported sfp as 'Supported SFP' and 
    unspported sfp as 'Not supported SFP'"""

    mask_supported = sfp_aggregated_modified_df['Transceiver_Supported'] == 'Yes'
    mask_not_supported = sfp_aggregated_modified_df['Transceiver_Supported'] == 'No'
    sfp_aggregated_modified_df.loc[mask_supported, 'Transceiver_Supported_stats'] = 'Supported SFP'
    sfp_aggregated_modified_df.loc[mask_not_supported, 'Transceiver_Supported_stats'] = 'Unsupported SFP'
    sfp_aggregated_modified_df['Transceiver_Supported_stats'].fillna(
        sfp_aggregated_modified_df['Transceiver_Supported'], inplace=True)


def comment_specific_sfp(sfp_df, sfp_specification_column: str, sfp_specification_name: 
                        str, normal_value: str, upper_case_spec: bool=False):
    """Function to filter off transceivers with normal specification value (short wave, normal distance) 
    in sfp_specification_column and add specification name to the value"""

    # column with commented specific sfp values
    special_sfp_column = sfp_specification_column + '_stats'    
    # check if specific values except normal is in sfp_specification_column
    if sfp_specification_column in sfp_df.columns \
        and sfp_df[sfp_specification_column].nunique() > 1: 
            # filter for normal sfps
            mask_normal_sfp = sfp_df[sfp_specification_column] == normal_value
            # comment specific sfps
            sfp_df.loc[~mask_normal_sfp, special_sfp_column] = sfp_specification_name + ' ' + (
                sfp_df[sfp_specification_column].str.upper() if upper_case_spec else sfp_df[sfp_specification_column])
    else:
        sfp_df[special_sfp_column] = np.nan


def count_sfp_statistics(portshow_sfp_aggregated_df, pattern_dct):
    """Function to count values in sfp_stats_columns"""

    sfp_aggregated_modified_df = sfp_prior_preparation(portshow_sfp_aggregated_df, pattern_dct)

    # columns to distinguish single switch
    switch_columns = ['Fabric_name', 'Fabric_label', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn']
    # columns to count values statistics in
    sfp_stats_columns = ['Port_quantity', 'Port_license', 'Transceiver_quantity', 
                         'Transceiver_Supported_stats', 'Transceiver_form_factor_stats', 'Transceiver_distanceMax_stats', 
                        'Transceiver_speed_mode_extracted', 'Transceiver_Name_PN',
                        'PortPhys_transceiver',
                        'RX_Power_dBm_stats', 'TX_Power_dBm_stats', 'RX_Power_uW_stats', 'TX_Power_uW_stats',
                        'Temperature_Centigrade_stats', 'Pwr_On_Time_years_stats']

    # count statistics on switch level
    sfp_statistics_switch_df = dfop.count_statistics(
        sfp_aggregated_modified_df, connection_grp_columns=switch_columns, stat_columns=sfp_stats_columns)
    # count licensed ports wo transceiverS
    
    if  'Port does not use an SFP or is disabled' in sfp_statistics_switch_df.columns:
        sfp_statistics_switch_df['Port does not use an SFP or is disabled'].fillna(0, inplace=True)
        # number of licensed ports greater of equal port wo sfp
        mask_lic_ge_nosfp = sfp_statistics_switch_df['Licensed'].ge(sfp_statistics_switch_df[['Transceiver_quantity', 'Port does not use an SFP or is disabled']].sum(axis=1))
        sfp_statistics_switch_df.loc[mask_lic_ge_nosfp, 'Licensed_wo_SFP'] = \
            sfp_statistics_switch_df.loc[mask_lic_ge_nosfp, 'Licensed'] - sfp_statistics_switch_df.loc[mask_lic_ge_nosfp, ['Transceiver_quantity', 'Port does not use an SFP or is disabled']].sum(axis=1)
    else:
        # number of licensed ports greater of equal port wo sfp
        mask_lic_ge_nosfp = sfp_statistics_switch_df['Licensed'].ge(sfp_statistics_switch_df['Transceiver_quantity'])
        sfp_statistics_switch_df.loc[mask_lic_ge_nosfp, 'Licensed_wo_SFP'] = \
            sfp_statistics_switch_df.loc[mask_lic_ge_nosfp, 'Licensed'] - sfp_statistics_switch_df.loc[mask_lic_ge_nosfp, 'Transceiver_quantity']
    
    # count statistics on fabric name and fabric label levels
    sfp_statistics_summary_df = dfop.count_summary(sfp_statistics_switch_df, group_columns=['Fabric_name', 'Fabric_label'])
    # count statistics for total ports
    sfp_statistics_all_df = dfop.count_all_row(sfp_statistics_summary_df)
    # concatenate statistics
    sfp_statistics_df = dfop.concat_statistics(
        sfp_statistics_switch_df, sfp_statistics_summary_df, sfp_statistics_all_df, sort_columns=switch_columns)
    # move sfp free ports column
    sfp_statistics_df = dfop.move_column(sfp_statistics_df, 
                                         cols_to_move=['Licensed_wo_SFP', 'No_Module', 'No SFP installed in port', 
                                                       'Port does not use an SFP or is disabled'], ref_col='Transceiver_quantity')
    return sfp_statistics_df