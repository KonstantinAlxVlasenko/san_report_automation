"""Module to fill ISL links and switches information in portcmd DataFrame"""

import numpy as np

import utilities.dataframe_operations as dfop


def switchparams_join(portshow_aggregated_df, switch_params_df, switch_params_aggregated_df):
    """
    Function to label switches in portshow_aggregated_df with Fabric names and labels.
    Add switchState, switchMode and Generation information
    """
    
    # add 'switch_index', 'switchState', 'switchMode' from switch_params_df
    # columns labels reqiured for join operation
    switchparams_lst = ['configname', 'chassis_name', 'chassis_wwn', 
                        'switchName', 'switchWwn',
                        'switch_index', 'switchState', 'switchMode', 'fabric.domain']

    switchparams_join_df = switch_params_df.loc[:, switchparams_lst].copy()
    portshow_aggregated_df = portshow_aggregated_df.merge(switchparams_join_df, how = 'left', on = switchparams_lst[:5])

    # add switch Generation based on chassis wwn
    switch_generation_lst = ['configname', 'chassis_name', 'chassis_wwn', 'switchType', 'Generation', 'HPE_modelName', 'switchClass']
    switch_generation_df = switch_params_aggregated_df.loc[:, switch_generation_lst].copy()
    switch_generation_df.drop_duplicates(subset=switch_generation_lst[:3], inplace=True)
    portshow_aggregated_df = portshow_aggregated_df.merge(switch_generation_df, how='left', on=switch_generation_lst[:3])

    # join Domain_ID and PortIndex columns
    mask_notna = portshow_aggregated_df[['fabric.domain', 'portIndex']].notna().all(axis=1)
    portshow_aggregated_df.loc[mask_notna, 'Domain_Index'] = portshow_aggregated_df['fabric.domain'] + ',' + portshow_aggregated_df['portIndex']
    return portshow_aggregated_df


def switchshow_join(portshow_df, switchshow_df):
    """Function to add switch information to portshow DataFrame
    Adding switchName, switchWwn, speed and portType
    Initially DataFrame contains only chassisName and chassisWwn
    Merge DataFrames on configName, chassisName, chassisWwn, slot and port"""
    
    # columns labels reqiured for join operation
    switchshow_lst = ['configname', 'chassis_name', 'chassis_wwn', 'slot', 'port', 'switchName', 
                      'switchWwn', 'speed', 'portType', 'connection_details']
    # create left DataFrame for join operation
    switchshow_join_df = switchshow_df.loc[:, switchshow_lst].copy()
    # portshow_df and switchshow_join_df DataFrames join operation
    portshow_aggregated_df = portshow_df.merge(switchshow_join_df, how = 'left', on = switchshow_lst[:5])
    return portshow_aggregated_df


def fill_isl_link(portshow_aggregated_df, isl_aggregated_df):
    """Function to add ISL links information (switchname, port, ip, model) to portcmd DataFrame"""

    if not isl_aggregated_df.empty:
        # extract required columns from isl_aggregated_df
        isl_columns_lst = ['Fabric_name', 'Fabric_label', 'configname',	'chassis_name', 
                            'SwitchName', 'switchWwn', 'portIndex',	'slot',	'port', 
                            'Connected_SwitchName',  'Connected_portIndex',	
                            'Connected_slot', 'Connected_port', 
                            'Connected_HPE_modelName', 'Connected_Enet_IP_Addr']
        isl_join_df = isl_aggregated_df.loc[:, isl_columns_lst].copy()

        # merge portIndex, slot and port number of Connected port
        isl_join_df = isl_join_df.astype({'Connected_portIndex': 'str', 
                                            'Connected_slot': 'str', 'Connected_port': 'str'}, errors = 'ignore')
        isl_join_df['Device_Port'] = isl_join_df.Connected_portIndex + '-' + \
            isl_join_df.Connected_slot + '-' + isl_join_df.Connected_port

        # rename columns to correspond columns in portshow_aggregated_df
        isl_columns_dct = {'SwitchName': 'switchName', 
                        'Connected_SwitchName': 'Device_Host_Name', 
                        'Connected_HPE_modelName': 'Device_Model',
                        'Connected_Enet_IP_Addr': 'IP_Address'}
        isl_join_df.rename(columns = isl_columns_dct, inplace=True)

        # isl_join_df columns required to fill empty values in portshow_aggregated_df 
        isl_columns_lst = ['Fabric_name', 'Fabric_label', 
                            'configname', 'chassis_name',
                            'switchName', 'switchWwn',
                            'portIndex', 'slot', 'port',
                            'Device_Host_Name', 'Device_Model',
                            'Device_Port', 'IP_Address']
        isl_join_df = isl_join_df.reindex(columns = isl_columns_lst)

        # fill empty values in portshow_aggregated_df from isl_join_df
        portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, isl_join_df, 
                                                    join_lst = isl_columns_lst[:9], filled_lst = isl_columns_lst[9:])
    return portshow_aggregated_df


def fill_switch_info(portshow_aggregated_df, switch_params_df, switch_params_aggregated_df):
    """
    Function to add connected switch information (SN, IP, Location, FOS, Model) to portcmd DataFrame 
    based on combination of connected oui and switch main board seral number (oui_board_sn)
    """
    
    switch_params_aggregated_df['switchName'].fillna(switch_params_aggregated_df['SwitchName'], inplace=True)

    # generate combination of oui and switch main board seral number based on Connected port WWN
    portshow_aggregated_df['oui_board_sn'] = portshow_aggregated_df.Connected_portWwn_switchshow_filled.str.slice(start = 6)
    switch_params_aggregated_df['oui_board_sn'] = switch_params_aggregated_df.switchWwn.str.slice(start = 6)

    # extract required columns from switch_params_aggregated_df
    switch_params_columns_lst = ['Fabric_name', 'Fabric_label', 'oui_board_sn',
                                'switchName', 'boot.ipa',
                                'ssn', 'FOS_version',
                                'Brocade_modelName', 'HPE_modelName', 'Device_Location']
    switch_params_join_df = switch_params_aggregated_df.loc[:, switch_params_columns_lst].copy()
    switch_params_join_df['HPE_modelName'].replace('^-$', np.nan, regex=True, inplace=True)
    # switch_params_join_df['HPE_modelName'].replace('-', np.nan, inplace=True)
    switch_params_join_df['HPE_modelName'].fillna(switch_params_join_df['Brocade_modelName'], inplace=True)
    switch_params_join_df.drop(columns=['Brocade_modelName'], inplace=True)

    # rename columns to correspond columns in portshow_aggregated_df
    switch_params_columns_dct = {
                                'switchName': 'Device_Host_Name', 'boot.ipa': 'IP_Address',
                                'ssn': 'Device_SN', 'FOS_version': 'Device_Fw',
                                'HPE_modelName': 'Device_Model'
                                }
    switch_params_join_df.rename(columns = switch_params_columns_dct, inplace=True)
    
    # # fill empty values in portshow_aggregated_df from switch_params_join_df
    switch_join_columns_lst = switch_params_join_df.columns.to_list()
    portshow_aggregated_df['Device_Model'].replace('^-$', np.nan, regex=True, inplace=True)
    # portshow_aggregated_df['Device_Model'].replace('^-$', np.nan, regex=True, inplace=True)
    portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, switch_params_join_df, 
                                                join_lst = switch_join_columns_lst[:3], filled_lst = switch_join_columns_lst[3:])
    portshow_aggregated_df = switch_name_correction(portshow_aggregated_df, switch_params_aggregated_df)
    return portshow_aggregated_df


def switch_name_correction(portshow_aggregated_df, switch_params_aggregated_df):
    """Function to correct names of the switches extracted from NodeSymb.
    This name might differ from one defined in switchName. Function replaces name extracted 
    from NodeSymb with switcName from swithc_patametes_aggregated_df."""

    # 
    switch_name_wwn_columns = ['Fabric_name', 'Fabric_label', 'switchName', 'switchWwn']
    switch_name_wwn_df = switch_params_aggregated_df[switch_name_wwn_columns].copy()
    switch_name_wwn_df.dropna(subset=['switchName', 'switchWwn'], inplace=True)

    rename_columns_dct = {'switchName': 'Device_Host_Name', 'switchWwn': 'NodeName'}
    switch_name_wwn_df.rename(columns=rename_columns_dct, inplace=True)

    portshow_aggregated_df['Device_Host_Name_tmp'] = portshow_aggregated_df['Device_Host_Name']
    portshow_aggregated_df['Device_Host_Name'] = np.nan

    portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, switch_name_wwn_df,
                                                join_lst=['Fabric_name', 'Fabric_label', 'NodeName'],
                                                filled_lst=['Device_Host_Name'])
    
    portshow_aggregated_df['Device_Host_Name'] = portshow_aggregated_df['Device_Host_Name'].fillna(portshow_aggregated_df['Device_Host_Name_tmp'])
    # portshow_aggregated_df['Device_Host_Name'].fillna(portshow_aggregated_df['Device_Host_Name_tmp'], inplace=True) depricared method
    portshow_aggregated_df.drop(columns=['Device_Host_Name_tmp'], inplace=True)
    return portshow_aggregated_df


def verify_port_license(portshow_aggregated_df):
    """Function to check if switch port is licensed"""

    mask_not_licensed = portshow_aggregated_df['connection_details'].str.contains('no (?:pod|(?:qflex )?ports on demand) license', case=False)
    mask_connection_detail_notna = portshow_aggregated_df['connection_details'].notna()
    portshow_aggregated_df['Port_license'] = np.where(mask_connection_detail_notna & mask_not_licensed, 'Not_licensed', 'Licensed')