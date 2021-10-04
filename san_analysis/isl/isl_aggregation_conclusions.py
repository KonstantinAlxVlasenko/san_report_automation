"""Module to count isl link attenuation and 
find unequal port configuration parameters from both sides of isl link""" 

import numpy as np
import pandas as pd

from common_operations_dataframe import dataframe_fillna, сoncatenate_columns


def attenuation_calc(isl_aggregated_df):
    """Function to calculate ISL link signal attenuation"""

    # switch ports power values column names
    sfp_power_lst = ['RX_Power_dBm', 'TX_Power_dBm', 'RX_Power_uW', 'TX_Power_uW']
    # connected switch ports power values column names
    sfp_power_connected_lst = [ 'Connected_TX_Power_dBm', 'Connected_RX_Power_dBm', 'Connected_TX_Power_uW', 'Connected_RX_Power_uW', ]
    # type of attenuation column names
    sfp_attenuation_lst = ['In_Attenuation_dB', 'Out_Attenuation_dB', 'In_Attenuation_dB(lg)', 'Out_Attenuation_dB(lg)']
    
    # turn off division by zero check due to some power values might be 0
    # and mask_notzero apllied after division calculation
    np.seterr(divide = 'ignore')
    
    for attenuation_type, power, connected_power in zip(sfp_attenuation_lst, sfp_power_lst, sfp_power_connected_lst):
        # empty values mask
        mask_notna = isl_aggregated_df[[power, connected_power]].notna().all(1)
        # inifinite values mask
        mask_finite = np.isfinite(isl_aggregated_df[[power, connected_power]]).all(1)
        # zero values mask
        mask_notzero = (isl_aggregated_df[[power, connected_power]] != 0).all(1)
        
        # incoming signal attenuation calculated using dBm values
        if attenuation_type == 'In_Attenuation_dB':
            isl_aggregated_df.loc[mask_notna & mask_finite & mask_notzero, attenuation_type] = \
            isl_aggregated_df[connected_power] - isl_aggregated_df[power]
        # outgoing signal attenuation calculated using dBm values
        elif attenuation_type == 'Out_Attenuation_dB':
            isl_aggregated_df.loc[mask_notna & mask_finite & mask_notzero, attenuation_type] = \
            isl_aggregated_df[power] - isl_aggregated_df[connected_power]
        # incoming signal attenuation calculated using uW values
        elif attenuation_type == 'In_Attenuation_dB(lg)':
            isl_aggregated_df.loc[mask_notna & mask_finite & mask_notzero, attenuation_type] = \
            round(10 * np.log10(isl_aggregated_df[connected_power]/(isl_aggregated_df[power])), 1)
        # outgoing signal attenuation calculated using uW values
        elif attenuation_type == 'Out_Attenuation_dB(lg)':
            isl_aggregated_df.loc[mask_notna & mask_finite & mask_notzero, attenuation_type] = \
            round(10 * np.log10(isl_aggregated_df[power].div(isl_aggregated_df[connected_power])), 1)
    # turn on division by zero check    
    np.seterr(divide = 'warn')
    return isl_aggregated_df


def verify_isl_cfg_equality(isl_aggregated_df):
    """Function to find port configuratin parameters which are not equal for both sides of ISL connection"""


    isl_cp_df = isl_aggregated_df.copy()
    # '..' means service or setting is in OFF state
    isl_cp_df.replace(to_replace='..', value='OFF', inplace=True)
    # join QOS_Port and QOS_E_Port columns
    isl_cp_df['QOS_Port'] = isl_cp_df['QOS_Port'].fillna(isl_cp_df['QOS_E_Port'])
    isl_cp_df['Connected_QOS_Port'] = isl_cp_df['Connected_QOS_Port'].fillna(isl_cp_df['Connected_QOS_E_Port'])
    # list of port setting to be vrified for equality from both sides of isl
    cfg_columns = ['Speed_Cfg', 'Trunk_Port',	'Long_Distance', 'VC_Link_Init', 
                    'Locked_E_Port', 'ISL_R_RDY_Mode',	'RSCN_Suppressed', 
                    'LOS_TOV_mode', 'QOS_Port', 'Rate_Limit', 
                    'Credit_Recovery', 'Compression', 'Encryption', '10G/16G_FEC', 
                    'Fault_Delay', 'TDZ_mode', 'Fill_Word(Current)', 'FEC', 'Wavelength_nm']

    for cfg in cfg_columns:
        # column names with current main port and connected port configuration parameter of the switch
        connected_cfg = 'Connected_' + cfg
        # column names with current unequal main port and connected port configuration parameter
        unequal_cfg = cfg + '_unequal'
        connected_unequal_cfg = connected_cfg + '_unequal'
        # both ports must have port cfg parameters in some state (not na)
        mask_notna = isl_cp_df[[cfg, connected_cfg]].notna().all(axis=1)
        # parameter value is not equal for both sides of isl connection
        mask_differnt_cfg = isl_cp_df[cfg] !=  isl_cp_df[connected_cfg]
        # add parameter name and it's value to column with name containing parameter name 
        # and 'unequal' tag for main and connected ports
        isl_cp_df.loc[mask_notna & mask_differnt_cfg, unequal_cfg] = \
            cfg + ': ' + isl_cp_df[cfg].astype('str')
        isl_cp_df.loc[mask_notna & mask_differnt_cfg, connected_unequal_cfg] = \
            cfg + ': ' + isl_cp_df[connected_cfg].astype('str')
        
    # column names with unequal paremater names and values for main and connected ports
    unequal_cfg_columns = [cfg + '_unequal' for cfg in cfg_columns]
    connected_unequal_cfg_columns = ['Connected_' + cfg for cfg in unequal_cfg_columns]

    # join all columns with unequal parameters for main and connected ports separately
    isl_cp_df = сoncatenate_columns(isl_cp_df, summary_column='Unequal_cfg', 
                                                merge_columns=unequal_cfg_columns, sep=', ', 
                                                drop_merge_columns=True)
    isl_cp_df = сoncatenate_columns(isl_cp_df, summary_column='Connected_Unequal_cfg', 
                                                merge_columns=connected_unequal_cfg_columns, sep=', ', 
                                                drop_merge_columns=True)

    # add unequal parameters values for both sides of the link to isl_aggregated_df
    isl_cfg_columns = ['configname', 'chassis_name', 'chassis_wwn',
                    'SwitchName', 'switchWwn', 'slot', 'port',
                    'Connected_SwitchName', 'Connected_switchWwn',
                    'Connected_slot', 'Connected_port',
                    'Unequal_cfg', 'Connected_Unequal_cfg']
    isl_aggregated_df = dataframe_fillna(isl_aggregated_df, isl_cp_df, 
                                        join_lst=isl_cfg_columns[:-2], filled_lst=isl_cfg_columns[-2:])
    return isl_aggregated_df