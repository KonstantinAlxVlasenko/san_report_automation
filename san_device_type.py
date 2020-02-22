"""Module to identify Fabric device types"""

import pandas as pd
import numpy as np
from files_operations import status_info, load_data, save_data 
from files_operations import force_extract_check, save_xlsx_file
from dataframe_operations import dataframe_join, dataframe_segmentation, dataframe_import


def device_type_main(portshow_df, switchshow_ports_df, switch_params_aggregated_df, nsshow_df, nscamshow_df, alias_df, fdmi_df, blade_servers_df, report_columns_usage_dct, report_data_lst):
    """Main function to add device types to portshow DataFrame"""
    
    # report_data_lst contains [customer_name, dir_report, dir_data_objects, max_title]
    
    print('\n\nSTEP 20. DEVICE TYPES TABLE ...\n')
    
    *_, max_title, report_steps_dct = report_data_lst
    # check if data already have been extracted
    data_names = ['device_types']
    # loading data if were saved on previous iterations 
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    device_type_df, = data_lst

    # data force extract check 
    # if data have been calculated on previous iterations but force key is ON 
    # then data re-calculated again and saved
    # force key for each DataFrame
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # check if data was loaded and not empty
    data_check = force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)  
    # flag if fabrics labels was forced to be changed 
    fabric_labels_change = True if report_steps_dct['fabric_labels'][1] else False
  
    # # get aggregated DataFrames
    # fabric_clean_df, isl_aggregated_df, fcredge_df = \
    #     isl_aggregated(fabricshow_ag_labels_df, switch_params_aggregated_df, 
    #     isl_df, trunk_df, fcredge_df, sfpshow_df, portcfgshow_df, switchshow_ports_df)
    # # export DataFrame to excel if required
    # save_xlsx_file(isl_aggregated_df, 'isl_aggregated', report_data_lst, report_type = 'analysis')

    # when no data saved or force extract flag is on or fabric labels have been changed than 
    # analyze extracted config data  
    if not all(data_check) or any(force_extract_keys_lst) or fabric_labels_change:
        # information string if fabric labels force changed was initiated
        # and statistics recounting required
        if fabric_labels_change and not any(force_extract_keys_lst) and all(data_check):
            info = f'Force data processing due to change in Fabrics labeling'
            print(info, end =" ")
            status_info('ok', max_title, len(info))

        oui_df = dataframe_import('oui', max_title) 
        # current operation information string
        info = f'Generating Device types table'
        print(info, end =" ") 

        
        portshow_aggregated_df = portshow_aggregated(portshow_df, switchshow_ports_df, switch_params_aggregated_df, nsshow_df, nscamshow_df, alias_df, oui_df, fdmi_df, blade_servers_df)


        # after finish display status
        status_info('ok', max_title, len(info))
        # saving fabric_statistics and fabric_statistics_summary DataFrames to csv file
        save_data(report_data_lst, data_names, portshow_aggregated_df) 

    # save data to service file if it's required
    save_xlsx_file(portshow_aggregated_df, 'device_types', report_data_lst, report_type = 'analysis')

    return portshow_aggregated_df



def portshow_aggregated(portshow_df, switchshow_ports_df, switch_params_aggregated_df, nsshow_df, nscamshow_df, alias_df, oui_df, fdmi_df, blade_servers_df):
    # add switch information (switchName, portType, portSpeed) to portshow DataFrame
    portshow_aggregated_df = switchshow_join(portshow_df, switchshow_ports_df)
    # add fabric information (FabricName, FabricLabel) and switchMode to portshow_aggregated DataFrame
    portshow_aggregated_df = switchparams_join(portshow_aggregated_df, switch_params_aggregated_df)
    # prepare alias_df (label fabrics, replace WWNn with WWNp if present)
    alias_wwnp_df, alias_wwnn_wwnp_df, fabric_labels_df = alias_preparation(nsshow_df, alias_df, switch_params_aggregated_df)
    # prepare nscamshow_df (label fabrics, drop duplicates WWNp)
    nscamshow_join_df = nscamshow_preparation(nscamshow_df, fabric_labels_df)
    # add nsshow and aliases to portshow_df
    portshow_aggregated_df = alias_nsshow_join(portshow_aggregated_df, alias_wwnp_df, nscamshow_join_df)
    # add 'deviceType', 'deviceSubtype' columns
    portshow_aggregated_df = portshow_aggregated_df.reindex(columns=[*portshow_aggregated_df.columns.tolist(), 'deviceType', 'deviceSubtype'])
    # add preliminarily device type (SRV, STORAGE, LIB, SWITCH, VC) and subtype based on oui (WWNp)
    portshow_aggregated_df = oui_join(portshow_aggregated_df, oui_df)
    # # allocate oui and vendor information from  from Access Gateway switches WWNp
    # ag_oui = ag_switches_oui(ag_principal_df)
    # preliminarily assisgn to all initiators type SRV
    mask_initiator = portshow_aggregated_df.Device_type.isin(['Physical Initiator', 'NPIV Initiator'])
    portshow_aggregated_df.loc[mask_initiator, ['deviceType', 'deviceSubtype']] = ['SRV', 'SRV']
    # 
    switches_oui = switch_params_aggregated_df['switchWwn'].str.slice(start = 6)
    # final device type define
    # portshow_aggregated_df[['deviceType', 'deviceSubtype']] = portshow_aggregated_df.apply(lambda series: type_check(series, switches_oui, fdmi_df, blade_servers_df) if series[['type', 'subtype']].notnull().all() else pd.Series((np.nan, np.nan)), axis = 1)
    # portshow_aggregated_df[['deviceType', 'deviceSubtype']] = portshow_aggregated_df.apply(lambda series: type_check(series, switches_oui, fdmi_df, blade_servers_df) if series[['Connected_oui', 'Connected_portWwn']].notnull().all() else pd.Series((np.nan, np.nan)), axis = 1)
    portshow_aggregated_df[['deviceType', 'deviceSubtype']] = portshow_aggregated_df.apply(lambda series: type_check(series, switches_oui, fdmi_df, blade_servers_df), axis = 1)

    return portshow_aggregated_df


def switchshow_join(portshow_df, switchshow_df):
    """Function to add switch information to portshow DataFrame
    Adding switchName, switchWwn, speed and portType
    Initially DataFrame contains only chassisName and chassisWwn
    Merge DataFrames on configName, chassisName, chassisWwn, slot and port"""
    
    # columns labels reqiured for join operation
    switchshow_lst = ['configname', 'chassis_name', 'chassis_wwn', 'slot', 'port', 'switchName', 
                      'switchWwn', 'speed', 'portType']
    # create left DataFrame for join operation
    switchshow_join_df = switchshow_df.loc[:, switchshow_lst].copy()
    # portshow_df and switchshow_join_df DataFrames join operation
    portshow_aggregated_df = portshow_df.merge(switchshow_join_df, how = 'left', on = switchshow_lst[:5])
    # drop columns with empty WWN device column
    portshow_aggregated_df.dropna(subset = ['Connected_portWwn'], inplace = True)
    
    return portshow_aggregated_df


def switchparams_join(portshow_aggregated_df, switch_params_aggregated_df):
    """Function to label switches in portshow_aggregated_df with Fabric names and labels, switchMode"""
    
    # columns labels reqiured for join operation
    switchparams_lst = ['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn', 'Fabric_name', 'Fabric_label', 'switchMode']
    # create left DataFrame for join operation
    switchparams_join_df = switch_params_aggregated_df.loc[:, switchparams_lst].copy()
    # portshow_aggregated_df and switchshow_join_df DataFrames join operation
    portshow_aggregated_df = portshow_aggregated_df.merge(switchparams_join_df, how = 'left', on = switchparams_lst[:5])

    return portshow_aggregated_df


def alias_preparation(nsshow_df, alias_df, switch_params_aggregated_df):
    """Function to label aliases DataFrame and replace WWNn with WWNp if any"""
    
    # create fabric labels DataFrame
    fabric_labels_lst = ['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn', 'Fabric_name', 'Fabric_label']
    fabric_labels_df = switch_params_aggregated_df.loc[:, fabric_labels_lst].copy()

    # create local Name Server (NS) DataFrame 
    nsshow_lst = ['configname', 'chassis_name', 'chassis_wwn', 'SwitchName', 'switchWwn', 'PortName', 'NodeName']
    nsshow_join_df = nsshow_df.loc[:, nsshow_lst].copy()
    nsshow_join_df.rename(columns = {'SwitchName': 'switchName'}, inplace = True)
    # labeling Name Server DataFrame
    nsshow_join_df = nsshow_join_df.merge(fabric_labels_df, how = 'left', on = fabric_labels_lst[:5])
    # remove switch related columns from Name Server DataFrame to leave only Fabric labels, WWNp, WWNn
    # lowercase SwitchName
    nsshow_lst[3] = nsshow_lst[3][0].lower() + nsshow_lst[3][1:]
    nsshow_join_df.drop(columns = nsshow_lst[:5], inplace = True)
    
    # fabric labeling alias DataFrame
    alias_prep_df =  alias_df.rename(columns = {'principal_switch_name': 'switchName', 'principal_switchWwn': 'switchWwn'})
    alias_labeled_df = alias_prep_df.merge(fabric_labels_df, how = 'left', on = fabric_labels_lst[:5])
    # replacing WWNn with WWNp if any
    # create alias_join DataFrame
    alias_lst = ['Fabric_name', 'Fabric_label', 'alias', 'alias_member']
    alias_join_df = alias_labeled_df.loc[:, alias_lst].copy()
    # merging alias_join and nsshow_join DataFrame
    # if alias_member column contains WWNn then new column contains corresponding WWNp for that WWNn
    alias_wwnn_wwnp_df = alias_join_df.merge(nsshow_join_df, how = 'left', left_on = ['Fabric_name', 'Fabric_label', 'alias_member'], right_on = ['Fabric_name', 'Fabric_label', 'NodeName'])
    # fill empty cells in WWNn -> WWNp column with alias_member WWNp values thus filtering off all WWNn values
    alias_wwnp_df = alias_wwnn_wwnp_df.copy()   
    alias_wwnp_df.PortName.fillna(alias_join_df['alias_member'], inplace = True)

    # drop possibly mixed WWNp and WWNn column alias_memeber and pure WWNn column
    alias_wwnp_df.drop(columns = ['alias_member', 'NodeName'], inplace = True)
    # alias_wwnp_df = alias_join_df.loc[:, ['Fabric_name', 'Fabric_label', 'alias', 'PortName']]
    
    return alias_wwnp_df, alias_wwnn_wwnp_df, fabric_labels_df


def nscamshow_preparation(nscamshow_df, fabric_labels_df):
    """Function to label Remote devices in the Name Server (NS) cache DataFrame"""

    # create Remote devices in the Name Server (NS) cache DataFrame
    nscamshow_lst = ['configname', 'chassis_name', 'chassis_wwn', 'SwitchName', 'switchWwn', 'PortName', 'Device_type']
    nscamshow_join_df = nscamshow_df.loc[:, nscamshow_lst]
    # rename SwitchName columnname
    nscamshow_join_df.rename(columns = {'SwitchName': 'switchName'}, inplace = True)
    # lowercase SwitchName
    nscamshow_lst[3] = nscamshow_lst[3][0].lower() + nscamshow_lst[3][1:] 
    
    # label Remote devices in the Name Server (NS) cache with Fabric labels
    nscamshow_join_df = nscamshow_join_df.merge(fabric_labels_df, how = 'left', on = nscamshow_lst[:5])
    # drop switch information columns
    nscamshow_join_df.drop(columns = nscamshow_lst[:5], inplace= True)
    # drop duplicates WWNp
    nscamshow_join_df = nscamshow_join_df.drop_duplicates(subset = ['Fabric_name', 'Fabric_label', 'PortName'])
    
    return nscamshow_join_df


def alias_nsshow_join(portshow_aggregated_df, alias_wwnp_df, nscamshow_join_df):
    """Finction to add porttype (Target, Initiator) anf alias to portshow_aggregated DataFrame"""
    
    # adding porttype (Target, Initiator) to portshow_aggregated DataFrame
    portshow_aggregated_df = portshow_aggregated_df.merge(nscamshow_join_df, how = 'left', 
                                                          left_on = ['Fabric_name', 'Fabric_label', 'Connected_portWwn'], 
                                                          right_on = ['Fabric_name', 'Fabric_label', 'PortName'])
    
    # if switch in AG mode then device type must be replaced to Physical instead of NPIV
    mask_ag = portshow_aggregated_df.switchMode == 'Access Gateway Mode'
    portshow_aggregated_df.loc[mask_ag, 'Device_type'] = \
        portshow_aggregated_df.loc[mask_ag, 'Device_type'].str.replace('NPIV', 'Physical')

    # add aliases to portshow_aggregated_df
    portshow_aggregated_df = portshow_aggregated_df.merge(alias_wwnp_df, how = 'left', 
                                                          on = ['Fabric_name', 'Fabric_label', 'PortName'])
    
    return portshow_aggregated_df


def oui_join(portshow_aggregated_df, oui_df):
    """Function to add preliminarily device type (SRV, STORAGE, LIB, SWITCH, VC) and subtype based on oui (WWNp)"""  
    
    # allocate oui from WWNp
    portshow_aggregated_df['Connected_oui'] = portshow_aggregated_df.Connected_portWwn.str.slice(start = 6, stop = 14)
    # add device types from oui DataFrame
    portshow_aggregated_df = portshow_aggregated_df.merge(oui_df, how = 'left', on = ['Connected_oui'])
    
    return portshow_aggregated_df


# def ag_switches_oui(ag_principal_df):
#     """Function to allocate oui and vendor information from Access Gateway switches WWNp"""

#     if not ag_principal_df.empty:
#         return ag_principal_df['AG_Switch_WWN'].str.slice(start = 6)
#     else:
#         return ag_principal_df['AG_Switch_WWN']


def type_check(series, switches_oui, fdmi_df, blade_servers_df):
    
    dev_type = (np.nan, np.nan)
#    print(series)
    # drop rows with empty WWNp values
    blade_hba_df = blade_servers_df.dropna(subset = ['portWwn'])


    if series[['type', 'subtype']].notnull().all():

        # devices with strictly defined type and subtype
        if not '|' in series['type'] and not '|' in  series['subtype']:
            return pd.Series((series.type, series.subtype))
        # check SWITCH TYPE
        elif 'SRV|SWITCH' in series['type']:
            if 'E-Port' in series['portType']:
                return pd.Series(('SWITCH', series.subtype))
            elif (series['Connected_portWwn'][6:] == switches_oui).any():
                return pd.Series(('SWITCH', series.subtype))
            else:
                return pd.Series(('SRV', series.subtype))
        elif series['switchMode'] == 'Access Gateway Mode' and series['portType'] == 'N-Port':
            return pd.Series(('SWITCH', 'SWITCH'))
        
        # servers type
        # if WWNp in blade hba DataFrame
        elif (blade_hba_df['portWwn'] == series['Connected_portWwn']).any():
            return pd.Series(('SRV', series['subtype'].split('|')[0]))
        elif (fdmi_df['WWNp'] == series['Connected_portWwn']).any() and series['Device_type'] in ['Physical Initiator', 'NPIV Initiator']:
            return pd.Series(('SRV', series['subtype'].split('|')[0]))
        
        elif series['type'] == 'SRV|LIB':
            if series['Device_type'] in ['Physical Initiator', 'NPIV Initiator']:
                return pd.Series(('SRV', series['subtype'].split('|')[0]))
            elif series['Device_type'] in ['Physical Target', 'NPIV Target']:
                return pd.Series(('LIB', series['subtype'].split('|')[1]))
            
        elif series['type'] == 'SRV|STORAGE':
            if series['Device_type'] in ['Physical Target', 'NPIV Target']:
                return pd.Series(('STORAGE', series['subtype'].split('|')[1]))
            
        elif series['type'] == 'SRV|STORAGE|LIB':
            if series['Device_type'] in ['Physical Initiator', 'NPIV Initiator']:
                return pd.Series(('SRV', series['subtype'].split('|')[0]))
            elif series['Device_type'] in ['Physical Target', 'NPIV Target']:
                if not pd.isna(series['alias']) and 'esl' in series['alias'].lower():
                    return pd.Series(('LIB', 'ESL'))
                elif not pd.isna(series['alias']) and 'msl' in series['alias'].lower():
                    return pd.Series(('LIB', 'MSL'))
                elif not pd.isna(series['alias']) and ('so' in series['alias'].lower() or 'store' in series['alias'].lower()):
                    return pd.Series(('LIB', 'StoreOnce'))
            
            
            
#            return pd.Series(('LIB', series['subtype'].split('|')[1]))
        
#    elif series['type'] == 'STORAGE|LIB':
#         if not pd.isna(series['alias']) and 'msl' in series['alias'].lower():
#             return pd.Series(('LIB', series['subtype'].split('|')[1]))

    
    if pd.isna(series[['deviceType', 'deviceSubtype']]).any():
        if series[['type', 'subtype']].notnull().all():                                
            return pd.Series((series['type'], series['subtype']))
        else:
            return pd.Series((np.nan, np.nan))
    else:
        return pd.Series((series['deviceType'], series['deviceSubtype']))