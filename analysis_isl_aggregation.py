"""Module to get aggreagated ISL DataFrame"""


import numpy as np
import pandas as pd

from analysis_isl_aggregation_conclusions import (attenuation_calc,
                                                  verify_isl_cfg_equality)
from common_operations_dataframe import dataframe_join, dataframe_fillna


def isl_aggregated(fabric_labels_df, switch_params_aggregated_df, 
    isl_df, trunk_df, lsdb_df, fcredge_df, portshow_df, sfpshow_df, portcfgshow_df, switchshow_df, re_pattern_lst):
    """Function to create ISL aggregated DataFrame"""


    # remove unlabeled fabrics and slice DataFrame to drop unnecessary columns
    fabric_clean_df = fabric_clean(fabric_labels_df)
    # add switchnames to trunk and fcredge DataFrames
    isl_df, trunk_df, fcredge_df = switchname_join(fabric_clean_df, isl_df, trunk_df, fcredge_df)
    # add portIndex to IFL
    fcredge_df = fcredge_portindex_join(switchshow_df, fcredge_df)
    # number IFL links
    fcredge_df = number_ifl(fcredge_df, trunk_df)
    # add switch information to fcredge_df to concatenate it with isl_df
    fcredge_cp_df = fcredge_to_isl_compliance(fcredge_df, switch_params_aggregated_df)
    # if not fcredge_cp_df.empty:
    # add ifl to isl
    isl_df['IFL_number'] = np.nan
    isl_df = pd.concat([isl_df, fcredge_cp_df],join='inner', ignore_index=True)
    # outer join of isl + ifl with trunk DataFrames 
    isl_aggregated_df = trunk_join(isl_df, trunk_df)
    # add ISL number in case of trunk presence and remove ifl tag
    isl_aggregated_df['ISL_number'].fillna(method='ffill', inplace=True)
    mask_ifl = isl_aggregated_df['ISL_number'].str.contains('ifl', case=False, na=False)
    isl_aggregated_df.loc[mask_ifl, 'ISL_number'] = np.nan
    # adding switchshow port information to isl aggregated DataFrame
    isl_aggregated_df, fcredge_df = porttype_join(switchshow_df, isl_aggregated_df, fcredge_df)
    # add link cost
    isl_aggregated_df = verify_link_cost(isl_aggregated_df, lsdb_df)
    fcredge_df = verify_link_cost(fcredge_df, lsdb_df)
    # adding link distance information
    isl_aggregated_df = portshow_join(portshow_df, switchshow_df, isl_aggregated_df)
    # adding sfp information to isl aggregated DataFrame
    isl_aggregated_df = sfp_join(sfpshow_df, isl_aggregated_df, re_pattern_lst)
    # adding switch information to isl aggregated DataFrame
    isl_aggregated_df = switch_join(switch_params_aggregated_df, isl_aggregated_df)
    # adding portcfg information to isl aggregated DataFrame
    isl_aggregated_df = portcfg_join(portcfgshow_df, isl_aggregated_df)
    # adding fabric labels to isl aggregated DataFrame
    isl_aggregated_df, fcredge_df = fabriclabel_join(fabric_clean_df, isl_aggregated_df, fcredge_df)
    # calculate maximum link speed
    isl_aggregated_df = max_isl_speed(isl_aggregated_df)
    # calculate link attenuation
    isl_aggregated_df = attenuation_calc(isl_aggregated_df)
    # remove unlabeled switches from isl aggregated Dataframe
    isl_aggregated_df, fcredge_df = remove_empty_fabrics(isl_aggregated_df, fcredge_df)
    # verify which port settings differs from both side of ISL 
    isl_aggregated_df = verify_isl_cfg_equality(isl_aggregated_df)
    # sort isl_aggregated
    isl_aggregated_df = sort_isl(isl_aggregated_df, switch_params_aggregated_df)
    return isl_aggregated_df, fcredge_df
    

def remove_empty_fabrics(isl_aggregated_df, fcredge_df):
    """
    Function to remove switches which are not part of research
    and sort required switches
    """

    # drop switches with empty fabric labels
    isl_aggregated_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace = True)
    # sort switches by switch names 
    isl_aggregated_df.sort_values(by=['switchType', 'chassis_name', 'switch_index', 'Fabric_name', 'Fabric_label'], \
        ascending=[False, True, True, True, True], inplace=True)
    # reset indexes
    isl_aggregated_df.reset_index(inplace=True, drop=True)
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        fcredge_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace = True)
        fcredge_df.sort_values(by=['chassis_name', 'switch_index', 'Fabric_name', 'Fabric_label'], \
                                      ascending=[True, True, True, True], inplace=True)        
    return isl_aggregated_df, fcredge_df


def max_isl_speed(isl_aggregated_df):
    """
    Function to evaluate maximum available port speed
    and check if ISL link operates on maximum speed.
    Maximum available link speed is calculated as minimum of next values 
    speed_chassis1, speed_chassis2, max_sfp_speed_switch1, max_sfp_speed_switch2
    """

    # columns to check speed
    speed_lst = ['Transceiver_speedMax', 'Connected_Transceiver_speedMax', 
                 'switch_speedMax', 'Connected_switch_speedMax']
    
    # minimum of four speed columns
    mask_speed_notna = isl_aggregated_df[speed_lst].notna().all(axis=1)
    isl_aggregated_df.loc[mask_speed_notna, 'Link_speedMax'] = isl_aggregated_df.loc[mask_speed_notna, speed_lst].min(axis=1, numeric_only=True)
    # actual link speed
    isl_aggregated_df['Link_speedActual'] = isl_aggregated_df['speed'].str.extract(r'(\d+)').astype('float64')
    # mask to check speed in columns are not None values
    mask_speed = isl_aggregated_df[['Link_speedActual', 'Link_speedMax']].notna().all(1)
    # compare values in Actual and Maximum speed columns
    isl_aggregated_df.loc[mask_speed, 'Link_speedActualMax']  = \
        pd.Series(np.where(isl_aggregated_df['Link_speedActual'].eq(isl_aggregated_df['Link_speedMax']), 'Yes', 'No'))
    return isl_aggregated_df
    

def fabriclabel_join(fabric_clean_df, isl_aggregated_df, fcredge_df):
    """Adding Fabric labels and IP addresses to ISL aggregated and FCREdge DataFrame"""

    # column names list to slice fabric_clean DataFrame and join with isl_aggregated Dataframe 
    fabric_labels_lst = ['Fabric_name', 'Fabric_label', 'SwitchName', 'switchWwn']
    # addition fabric labels information to isl_aggregated and fcredge DataFrames 
    isl_aggregated_df = isl_aggregated_df.merge(fabric_clean_df.loc[:, fabric_labels_lst], 
                                                how = 'left', on = fabric_labels_lst[2:])
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        fcredge_df = fcredge_df.merge(fabric_clean_df.loc[:, fabric_labels_lst],
                                      how = 'left', on = fabric_labels_lst[2:])
    # column names list to slice fabric_clean DataFrame and join with isl_aggregated Dataframe
    switch_ip_lst = ['SwitchName', 'switchWwn', 'Enet_IP_Addr']
    # addition IP adddreses information to isl_aggregated DataFrame
    isl_aggregated_df = dataframe_join(isl_aggregated_df, fabric_clean_df,  switch_ip_lst, 2)
    return isl_aggregated_df, fcredge_df


def portcfg_join(portcfgshow_df, isl_aggregated_df):
    """Adding portcfg information to ISL aggregated DataFrame"""
    # column names list to slice portcfg DataFrame and join with isl_aggregated Dataframe
    portcfg_lst = ['SwitchName', 'switchWwn', 'slot', 'port', 'Octet_Speed_Combo', 'Speed_Cfg',  'Trunk_Port',
                   'Long_Distance', 'VC_Link_Init', 'Locked_E_Port', 'ISL_R_RDY_Mode', 'RSCN_Suppressed',
                   'LOS_TOV_mode', 'QOS_Port', 'QOS_E_Port', 'Rate_Limit', 'Credit_Recovery', 'Compression', 'Encryption', 
                   '10G/16G_FEC', 'Fault_Delay', 'TDZ_mode', 'Fill_Word(Current)', 'FEC']
    # addition portcfg port information to isl_aggregated DataFrame
    isl_aggregated_df = dataframe_join(isl_aggregated_df, portcfgshow_df, portcfg_lst, 4)
    return isl_aggregated_df


def switch_join(switch_params_aggregated_df, isl_sfp_connected_df):
    """Adding switch licenses, max speed, description"""

    # column names list to slice switch_params_aggregated DataFrame and join with isl_aggregated Dataframe
    switch_lst = ['SwitchName', 'switchWwn', 'switchType','licenses', 'switch_speedMax', 'HPE_modelName', 
                    'Base_Switch', 'Allow_XISL_Use', 'Base_switch_in_chassis']   
    # addition switch parameters information to isl_aggregated DataFrame
    isl_aggregated_df = dataframe_join(isl_sfp_connected_df, switch_params_aggregated_df, switch_lst, 2)
    # convert switchType column to float for later sorting
    isl_aggregated_df = isl_aggregated_df.astype(dtype = {'switchType': 'float64'}, errors = 'ignore')
    # check if Trunking lic present on both swithes in ISL link
    switch_trunking_dct = {'licenses' : 'Trunking_license', 'Connected_licenses' : 'Connected_Trunking_license'}
    for lic, trunking_lic in switch_trunking_dct.items():
        isl_aggregated_df[trunking_lic] = isl_aggregated_df.loc[isl_aggregated_df[lic].notnull(), lic].apply(lambda x: 'Trunking' in x)
        isl_aggregated_df[trunking_lic].replace(to_replace={True: 'Yes', False: 'No'}, inplace = True)
    return isl_aggregated_df


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
    isl_aggregated_df = dataframe_join(isl_aggregated_df, portshow_join_df, portshow_lst, 4)    
    return isl_aggregated_df

    
def sfp_join(sfpshow_df, isl_aggregated_df, re_pattern_lst):
    """Adding sfp infromation for both ports of the ISL link"""

    # regular expression patterns
    comp_keys, _, comp_dct = re_pattern_lst

    # column names list to slice sfphshow DataFrame and join with isl_aggregated Dataframe
    sfp_lst = ['SwitchName', 'switchWwn', 'slot', 'port', 'Transceiver_PN', 'Wavelength_nm', 
                'Transceiver_mode',	'RX_Power_dBm',	'TX_Power_dBm',	'RX_Power_uW', 'TX_Power_uW']
    # convert sfp power data to float
    sfp_power_dct = {sfp_power: 'float64' for sfp_power in sfp_lst[7:]}
    sfpshow_df = sfpshow_df.astype(dtype = sfp_power_dct, errors = 'ignore')

    # addition switchshow port information to isl_aggregated DataFrame
    isl_aggregated_df = dataframe_join(isl_aggregated_df, sfpshow_df, sfp_lst, 4)    
    #max Transceiver speed
    sfp_speed_dct = {
            'Transceiver_mode': 'Transceiver_speedMax', 
            'Connected_Transceiver_mode': 'Connected_Transceiver_speedMax'
            }
    # extract tranceivers speed and take max value
    for sfp, sfp_sp_max in sfp_speed_dct.items():
            # extract speed values
            # isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp].str.extract(r'^([\d,]+)_(?:Gbps|MB)') # TO_REMOVE
            sfp_speed_values_re = comp_dct.get('transceiver_speed_values')
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp].str.extract(sfp_speed_values_re)
            # split string to create list of available speeds
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp_sp_max].str.split(',')
            # if list exist (speeds values was found) then choose maximum 
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp_sp_max].apply(lambda x: max([int(sp) for sp in x]) if isinstance(x, list) else np.nan)
            # if speed in Mb/s then convert to Gb/s
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp_sp_max].apply(lambda x: x/100 if x >= 100 else x)
    return isl_aggregated_df
    
    
def porttype_join(switchshow_df, isl_aggregated_df, fcredge_df):
    """Adding slot, port, speed and portType information for both ports of the ISL and IFR link"""

    # raname switchName column to allign with isl_aggregated DataFrame
    switchshow_join_df = switchshow_df.rename(columns={'switchName': 'SwitchName'})
    # column names list to slice switchshow DataFrame and join with isl_aggregated Dataframe
    porttype_lst = ['SwitchName', 'switchWwn', 'portIndex', 'slot', 'port', 'speed', 'portType']
    # addition switchshow port information to isl_aggregated DataFrame
    isl_aggregated_df = dataframe_join(isl_aggregated_df, switchshow_join_df, porttype_lst, 3)   
    
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        
        # # add portIndex to fcredge
        # port_index_lst = ['SwitchName', 'switchWwn', 'slot', 'port', 'portIndex']
        # switchshow_portindex_df = switchshow_join_df.loc[:, port_index_lst].copy()
        # fcredge_df = fcredge_df.merge(switchshow_portindex_df, how = 'left', on= port_index_lst[:-1])
        
        # drop slot and port columns to avoid duplicate columns after dataframe function 
        fcredge_df.drop(columns = ['slot', 'port'], inplace = True)
        # addition switchshow port information to fcredge DataFrame
        fcredge_df = dataframe_join(fcredge_df, switchshow_join_df, porttype_lst, 3)
    return isl_aggregated_df, fcredge_df


def fcredge_portindex_join(switchshow_df, fcredge_df):
    """Adding portIndex to IFL link"""

    # raname switchName column to allign with isl_aggregated DataFrame
    switchshow_join_df = switchshow_df.rename(columns={'switchName': 'SwitchName'}).copy()
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        # add portIndex to fcredge
        port_index_lst = ['configname', 'chassis_name', 'chassis_wwn', 
                          'SwitchName', 'switchWwn', 'slot', 'port', 'portIndex']
        switchshow_portindex_df = switchshow_join_df.loc[:, port_index_lst].copy()
        fcredge_df = fcredge_df.merge(switchshow_portindex_df, how = 'left', on= port_index_lst[:-1])
        
    return fcredge_df


def number_ifl(fcredge_df, trunk_df):
    """Function to Number IFL links. Links inside trunking group have same IFL number"""

    if not fcredge_df.empty:
        # add trunk related columns
        fcredge_df = dataframe_fillna(fcredge_df, trunk_df, join_lst=['switchWwn', 'portIndex'], 
                                    filled_lst=['Trunking_GroupNumber', 'Master'])

        # filter master links only to perform numbering (trunkless link considered to be master)
        mask_not_trunk_member = fcredge_df['Trunking_GroupNumber'].isna()
        fcredge_df.loc[mask_not_trunk_member, 'Master'] = 'MASTER'
        mask_trunk_master = fcredge_df['Master'].str.contains('master', case=False, na=False)
        fcredge_master_df = fcredge_df.loc[mask_trunk_master].copy()
        # master link numbering



        fcredge_master_df['IFL_number'] = fcredge_master_df.groupby(by=['switchWwn'])['Master'].rank(method="first", ascending=True)
        # number all IFL links which are part of trunk group based on master link IFL number
        fcredge_df = dataframe_fillna(fcredge_df, fcredge_master_df, join_lst=['switchWwn', 'Trunking_GroupNumber'], filled_lst=['IFL_number'])
        # copy IFLs links number for links which are not part of trunk group
        fcredge_df = dataframe_fillna(fcredge_df, fcredge_master_df, join_lst=['switchWwn', 'portIndex'], filled_lst=['IFL_number'])

        fcredge_df['IFL_number'] = fcredge_df['IFL_number'].astype('int64', errors='ignore')
    return fcredge_df


def fcredge_to_isl_compliance(fcredge_df, switch_params_aggregated_df):
    """Function to add columns to fcredge_df to comply with isl_df columns.
    Add switchRole, FC_router, Connected_switchDID columns"""

    fcredge_cp_df = fcredge_df.copy()
    if not fcredge_cp_df.empty:
        # add switchRole, FC_router, Connected_switchDID
        switch_params_aggregated_cp_df = switch_params_aggregated_df.copy()
        switch_params_aggregated_cp_df['FC_router'] = switch_params_aggregated_cp_df['FC_Router']
        switch_params_aggregated_cp_df['Connected_switchWwn'] = switch_params_aggregated_cp_df['switchWwn']
        switch_params_aggregated_cp_df['Connected_switchDID'] = switch_params_aggregated_cp_df['fabric.domain']
        fcredge_cp_df = dataframe_fillna(fcredge_cp_df, switch_params_aggregated_cp_df, join_lst=['switchWwn'], 
                                    filled_lst=['switchRole', 'FC_router'])
        fcredge_cp_df = dataframe_fillna(fcredge_cp_df, switch_params_aggregated_cp_df, join_lst=['Connected_switchWwn'], 
                                    filled_lst=['Connected_switchDID'], remove_duplicates=True, drop_na=True)
        # tag IFL links to avoid its numbering as ISL
        fcredge_cp_df['ISL_number'] = 'ifl'
        # rename columns to correspond to isl_df columns
        fcredge_cp_df.rename(columns={'BB_Fabric_ID': 'FabricID',  'Flags': 'Parameters'}, inplace=True)
    return fcredge_cp_df


def fabric_clean(fabricshow_ag_labels_df):
    """Function to prepare fabricshow_ag_labels DataFrame for join operation"""

    # create copy of fabricshow_ag_labels DataFrame
    fabric_clean_df = fabricshow_ag_labels_df.copy()
    # remove switches which are not part of research 
    # (was not labeled during Fabric labeling procedure)
    fabric_clean_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace = True)
    # reset fabrics DataFrame index after droping switches
    fabric_clean_df.reset_index(inplace=True, drop=True)
    # extract required columns
    fabric_clean_df = fabric_clean_df.loc[:, ['Fabric_name', 'Fabric_label', 'Worldwide_Name', 'Name', 'Enet_IP_Addr']]
    # rename columns as in switch_params DataFrame
    fabric_clean_df.rename(columns={'Worldwide_Name': 'switchWwn', 'Name': 'SwitchName'}, inplace=True)
    return fabric_clean_df


def switchname_join(fabric_clean_df, isl_df, trunk_df, fcredge_df):
    """Function to add switchnames to Trunk and FCREdge DataFrames"""

    # slicing fabric_clean DataFrame 
    switch_name_df = fabric_clean_df.loc[:, ['switchWwn', 'SwitchName']].copy()
    switch_name_df.rename(
            columns={'switchWwn': 'Connected_switchWwn', 'SwitchName': 'Connected_SwitchName'}, inplace=True)
    # adding switchnames to trunk_df
    trunk_df = trunk_df.merge(switch_name_df, how = 'left', on='Connected_switchWwn')
    # adding switchnames to isl_df
    isl_df = isl_df.merge(switch_name_df, how = 'left', on='Connected_switchWwn')
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        fcredge_df = fcredge_df.merge(switch_name_df, how = 'left', on='Connected_switchWwn')
    return isl_df, trunk_df, fcredge_df


def trunk_join(isl_df, trunk_df):
    """
    Join Trunk and ISL DataFrames
    Add switcNames to Trunk and FCREdge DataFrames
    """
    
    # convert numerical data in ISL and TRUNK DataFrames to float
    isl_df = isl_df.astype(dtype = 'float64', errors = 'ignore')    
    trunk_df  = trunk_df.astype(dtype = 'float64', errors = 'ignore')
    
    # List of columns DataFrames are joined on     
    join_lst = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 'SwitchName',
                'switchWwn', 'switchRole', 'FabricID', 'FC_router', 'portIndex', 
                'Connected_portIndex', 'Connected_SwitchName',
                'Connected_switchWwn', 'Connected_switchDID']  

    # merge updated ISL and TRUNK DataFrames 
    isl_aggregated_df = trunk_df.merge(isl_df, how = 'outer', on = join_lst)
    return isl_aggregated_df


def verify_link_cost(df, lsdb_df):
    """Function to add link cost"""

    if not df.empty:
        mask_local_sw = lsdb_df['self_tag'].notna()
        lsdb_local_sw_df = lsdb_df.loc[mask_local_sw].copy()

        join_lst = ['SwitchName', 'switchWwn', 'portIndex', 'cost']
        df = dataframe_join(df, lsdb_local_sw_df, join_lst, 3)
    return df

def sort_isl(isl_aggregated_df, switch_params_aggregated_df):
    """Function to sort ISLs based on isl_sort_columns"""

    # adding switchtype to sort switches by model
    isl_aggregated_df = dataframe_fillna(isl_aggregated_df, switch_params_aggregated_df, filled_lst=['switchType'], join_lst=['switchWwn'])

    isl_sort_columns = ['Fabric_name', 'Fabric_label',
                        'switchType', 'chassis_name', 'chassis_wwn',
                        'SwitchName', 'switchWwn',
                        'Trunking_GroupNumber', 'Master',
                        'ISL_number', 'IFL_number', 'portIndex']
    isl_sort_columns = [column for column in isl_sort_columns if column in isl_aggregated_df.columns]
    order_lst = [column != 'switchType' for column in isl_sort_columns]
    isl_aggregated_df.sort_values(by=isl_sort_columns, ascending=order_lst, inplace=True)
    return isl_aggregated_df