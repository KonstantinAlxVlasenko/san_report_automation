"""Module to add ports configuration to aggregated portcmd DataFrame,
filter ports for which the error threshold has been exceeded"""

import pandas as pd


def port_cfg_join(portshow_aggregated_df, portcfgshow_df):
    """Function to add ports configuration to aggregated portcmd DataFrame"""
    
    join_columns_lst = ['configname', 'chassis_name', 'chassis_wwn', 
                        'switchName', 'switchWwn', 'slot', 'port']

    # rename columns in portcfgshow_df duplicated in portshow_aggregated_df DataFrame except columns to merge on
    duplicate_columns = [column for column in portcfgshow_df.columns if 
                            (column in portshow_aggregated_df.columns and not column in join_columns_lst)]
    duplicate_columns_rename = [column + '_cfg' for column in duplicate_columns]
    rename_dct = {k:v for k, v in zip(duplicate_columns, duplicate_columns_rename)}
    portcfgshow_df.rename(columns=rename_dct, inplace=True)
    # change column names to correspond portshow_aggregated_df
    portcfgshow_df.rename(columns={'SwitchName': 'switchName'}, inplace=True)
    # add portcfgshow to port_complete_df
    port_complete_df = portshow_aggregated_df.merge(portcfgshow_df, how='left', on=join_columns_lst).copy()
    port_complete_df.drop_duplicates(inplace=True)
    return port_complete_df


def port_error_filter(portshow_sfp_aggregated_df, error_threshhold_num: int=100, error_threshold_percenatge: int=3):
    """Function to filter ports for which the error threshold has been exceeded.
    For critical errors the threshold value is 100 errors and 
    for medium errors the threshold value is 3 percent of the number of received frames"""

    filtered_error_lst = []

    stat_frx = 'stat_frx'

    medium_errors = [
        ['Link_failure', 'Loss_of_sync', 'Loss_of_sig'],
        ['er_rx_c3_timeout', 'er_tx_c3_timeout', 'er_unroutable', 'er_unreachable', 'er_other_discard'],
        ['er_enc_in', 'er_enc_out', 'er_crc', 'er_bad_os'], 
        ['er_bad_eof']
        ]

    critical_errors = [
        ['Lr_in', 'Lr_out', 'Ols_in',	'Ols_out'], 
        ['er_crc_good_eof'], 
        ['fec_uncor_detected'], 
        ['er_pcs_blk']
        ]

    # convert error and received frames columns to numeric type
    errors_flat = [error for error_grp in [*critical_errors, *medium_errors] for error in error_grp]
    portshow_sfp_aggregated_df[[stat_frx, *errors_flat]] = portshow_sfp_aggregated_df[[stat_frx, *errors_flat]].apply(pd.to_numeric, errors='ignore')

    # create column with medium error percentage from number of received frames
    medium_errors_flat = [error for error_grp in medium_errors for error in error_grp]
    for err_column in medium_errors_flat:
        err_percentage_column = err_column + '_percentage'
        portshow_sfp_aggregated_df[err_percentage_column] = (portshow_sfp_aggregated_df[err_column] / portshow_sfp_aggregated_df[stat_frx]) * 100
        portshow_sfp_aggregated_df[err_percentage_column] = portshow_sfp_aggregated_df[err_percentage_column].round(2)

    switch_columns = ['Fabric_name', 'Fabric_label', 
                        'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn',
                        'portIndex', 'slot', 'port', 'switchName_Index_slot_port', 'portState', 'portType',
                        'Device_Host_Name_Port_group', 'alias_Port_group', 'stat_frx']

    # verify critical errors which exceeds threshold
    for error_grp in critical_errors:
        mask_errors_num = (portshow_sfp_aggregated_df[error_grp] > error_threshhold_num).any(axis=1)
        filtered_error_df = portshow_sfp_aggregated_df.loc[mask_errors_num, [*switch_columns, *error_grp]]
        filtered_error_df.drop_duplicates(inplace=True)
        filtered_error_lst.append(filtered_error_df)

    # verify medium errors which exceeds thresholds
    for error_grp in medium_errors:
        mask_errors_num = (portshow_sfp_aggregated_df[error_grp] > error_threshhold_num).any(axis=1)
        error_grp_percantage = [error + '_percentage' for error in error_grp]
        mask_errors_percentage = (portshow_sfp_aggregated_df[error_grp_percantage] > error_threshold_percenatge).any(axis=1)
        filtered_error_df = portshow_sfp_aggregated_df.loc[mask_errors_num & mask_errors_percentage, [*switch_columns, *error_grp, *error_grp_percantage]]
        filtered_error_df.drop_duplicates(inplace=True)
        filtered_error_lst.append(filtered_error_df)
    return filtered_error_lst