"""Module to add information to aggreagated ISL DataFrame for each port"""


import numpy as np

import utilities.dataframe_operations as dfop


def port_level_join(isl_aggregated_df, fcredge_df, switchshow_df, portshow_df, portcfgshow_df, sfpshow_df, lsdb_df, pattern_dct):
    """Function to add information for each isl port"""

    # adding slot, port, speed and portType information to isl aggregated DataFrame
    isl_aggregated_df, fcredge_df = porttype_join(switchshow_df, isl_aggregated_df, fcredge_df)
    # add link cost
    isl_aggregated_df = link_cost_join(isl_aggregated_df, lsdb_df)
    fcredge_df = link_cost_join(fcredge_df, lsdb_df)
    # adding link distance information
    isl_aggregated_df = portshow_join(portshow_df, switchshow_df, isl_aggregated_df)
    # adding sfp information to isl aggregated DataFrame
    isl_aggregated_df = sfp_join(sfpshow_df, isl_aggregated_df, pattern_dct)
    # adding portcfg information to isl aggregated DataFrame
    isl_aggregated_df = portcfg_join(portcfgshow_df, isl_aggregated_df)
    return isl_aggregated_df, fcredge_df


def porttype_join(switchshow_df, isl_aggregated_df, fcredge_df):
    """Adding slot, port, speed and portType information for both ports of the ISL and IFR link"""

    # raname switchName column to allign with isl_aggregated DataFrame
    switchshow_join_df = switchshow_df.rename(columns={'switchName': 'SwitchName'})
    # column names list to slice switchshow DataFrame and join with isl_aggregated Dataframe
    porttype_lst = ['SwitchName', 'switchWwn', 'portIndex', 'slot', 'port', 'speed', 'portType']
    # addition switchshow port information to isl_aggregated DataFrame
    isl_aggregated_df = dfop.dataframe_join(isl_aggregated_df, switchshow_join_df, porttype_lst, 3)   
    
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        # drop slot and port columns to avoid duplicate columns after dataframe function 
        fcredge_df.drop(columns = ['slot', 'port'], inplace = True)
        # addition switchshow port information to fcredge DataFrame
        fcredge_df = dfop.dataframe_join(fcredge_df, switchshow_join_df, porttype_lst, 3)
    return isl_aggregated_df, fcredge_df


def link_cost_join(df, lsdb_df):
    """Function to add link cost"""

    if not df.empty:
        mask_local_sw = lsdb_df['self_tag'].notna()
        lsdb_local_sw_df = lsdb_df.loc[mask_local_sw].copy()

        join_lst = ['SwitchName', 'switchWwn', 'portIndex', 'cost']
        df = dfop.dataframe_join(df, lsdb_local_sw_df, join_lst, 3)
    return df


def portshow_join(portshow_df, switchshow_df, isl_aggregated_df):
    """Adding isl distance infromation for both ports of the ISL link"""

    # add switchname and switchwwn information to portshow_df
    # columns labels reqiured for join operation
    switchshow_lst = ['configname', 'chassis_name', 'chassis_wwn', 'slot', 'port', 'switchName', 
                      'switchWwn', 'speed', 'portType']
    # create left DataFrame for join operation
    switchshow_join_df = switchshow_df.loc[:, switchshow_lst].copy()
    # portshow_df and switchshow_join_df DataFrames join operation
    portshow_join_df = portshow_df.merge(switchshow_join_df, how = 'left', on = switchshow_lst[:5])
    portshow_join_df.rename(columns={'switchName': 'SwitchName'}, inplace=True)

    # add distance information from portshow_join_df to isl_aggregated_df
    # column names list to slice sfphshow DataFrame and join with isl_aggregated Dataframe
    portshow_lst = ['SwitchName', 'switchWwn', 'slot', 'port', 'Distance']
    # addition switchshow port information to isl_aggregated DataFrame
    isl_aggregated_df = dfop.dataframe_join(isl_aggregated_df, portshow_join_df, portshow_lst, 4)    
    return isl_aggregated_df

    
def sfp_join(sfpshow_df, isl_aggregated_df, pattern_dct):
    """Adding sfp infromation for both ports of the ISL link"""

    # column names list to slice sfphshow DataFrame and join with isl_aggregated Dataframe
    sfp_lst = ['SwitchName', 'switchWwn', 'slot', 'port', 'Transceiver_PN', 'Wavelength_nm', 
                'Transceiver_mode',	'RX_Power_dBm',	'TX_Power_dBm',	'RX_Power_uW', 'TX_Power_uW']
    # convert sfp power data to float
    sfp_power_dct = {sfp_power: 'float64' for sfp_power in sfp_lst[7:]}
    sfpshow_df = sfpshow_df.astype(dtype = sfp_power_dct, errors = 'ignore')

    # addition switchshow port information to isl_aggregated DataFrame
    isl_aggregated_df = dfop.dataframe_join(isl_aggregated_df, sfpshow_df, sfp_lst, 4)    
    #max Transceiver speed
    sfp_speed_dct = {
            'Transceiver_mode': 'Transceiver_speedMax', 
            'Connected_Transceiver_mode': 'Connected_Transceiver_speedMax'
            }
    # extract tranceivers speed and take max value
    for sfp, sfp_sp_max in sfp_speed_dct.items():
            # extract speed values
            sfp_speed_values_re = pattern_dct['transceiver_speed_values']
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp].str.extract(sfp_speed_values_re)
            # split string to create list of available speeds
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp_sp_max].str.split(',')
            # if list exist (speeds values was found) then choose maximum 
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp_sp_max].apply(
                lambda x: max([int(sp) for sp in x]) if isinstance(x, list) else np.nan)
            # if speed in Mb/s then convert to Gb/s
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp_sp_max].apply(lambda x: x/100 if x >= 100 else x)
    return isl_aggregated_df


def portcfg_join(portcfgshow_df, isl_aggregated_df):
    """Adding portcfg information to ISL aggregated DataFrame"""
    
    # column names list to slice portcfg DataFrame and join with isl_aggregated Dataframe
    portcfg_lst = ['SwitchName', 'switchWwn', 'slot', 'port', 'Octet_Speed_Combo', 'Speed_Cfg',  'Trunk_Port',
                   'Long_Distance', 'VC_Link_Init', 'Locked_E_Port', 'ISL_R_RDY_Mode', 'RSCN_Suppressed',
                   'LOS_TOV_mode', 'QOS_Port', 'QOS_E_Port', 'Rate_Limit', 'Credit_Recovery', 'Compression', 'Encryption', 
                   '10G/16G_FEC', 'Fault_Delay', 'TDZ_mode', 'Fill_Word(Current)', 'FEC']
    # addition portcfg port information to isl_aggregated DataFrame
    isl_aggregated_df = dfop.dataframe_join(isl_aggregated_df, portcfgshow_df, portcfg_lst, 4)
    return isl_aggregated_df