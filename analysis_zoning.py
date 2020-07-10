"""
Module to create zoning configuration related DataFrames
"""

import numpy as np
import pandas as pd

from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_dataframe import dataframe_join


def zoning_analysis_main(switch_params_aggregated_df, portshow_aggregated_df, 
                            cfg_df, zone_df, alias_df, cfg_effective_df, fcrfabric_df, lsan_df,
                            report_columns_usage_dct, report_data_lst):
    """Main function to analyze zoning configuration"""
    
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = [
        'zoning_aggregated'
        ]
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    zoning_aggregated_df, = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['cfg', 'cfg_effective', 'zone', 'alias',
                           'switch_params_aggregated', 'switch_parameters', 'switchshow_ports', 'chassis_parameters', 
                            'portshow_aggregated', 'portcmd', 'fdmi', 'nscamshow', 'nsshow', 'blade_servers', 'fabric_labels']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating zoning table'
        print(info, end =" ") 

        zoning_aggregated_df = zoning_aggregated(switch_params_aggregated_df, portshow_aggregated_df, 
                                                    cfg_df, zone_df, alias_df, cfg_effective_df, fcrfabric_df, lsan_df, report_data_lst)

        # after finish display status
        status_info('ok', max_title, len(info))

        # servers_report_df, storage_report_df, library_report_df, hba_report_df, \
        #     storage_connection_df,  library_connection_df, server_connection_df, npiv_report_df = \
        #         create_report_tables(portshow_aggregated_df, data_names[1:], \
        #             report_columns_usage_dct, max_title)

        # create list with partitioned DataFrames
        data_lst = [
            zoning_aggregated_df
            ]

        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)
        # save_xlsx_file(alias_wwnn_wwnp_df, 'alias_wwnn', report_data_lst)
        # save_xlsx_file(nsshow_unsplit_df, 'nsshow_unsplit', report_data_lst)
        # save_xlsx_file(expected_ag_links_df, 'expected_ag_links_df', report_data_lst)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        zoning_aggregated_df, = verify_data(report_data_lst, data_names, *data_lst)
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return zoning_aggregated_df


def zoning_aggregated(switch_params_aggregated_df, portshow_aggregated_df, 
                        cfg_df, zone_df, alias_df, cfg_effective_df, fcrfabric_df, lsan_df, report_data_lst):




    zoning_aggregated_df = zoning_from_configuration(switch_params_aggregated_df, cfg_df, cfg_effective_df, zone_df, alias_df)





    # wwnn_lst = sorted(portshow_aggregated_df.NodeName.dropna().drop_duplicates().to_list())
    # wwnp_lst = sorted(portshow_aggregated_df.PortName.dropna().drop_duplicates().to_list())
    # wwn_intersection = set(wwnp_lst).intersection(wwnn_lst)
    # wwnp_clear_lst = [wwnp for wwnp in wwnp_lst if wwnp not in wwn_intersection]
    # wwnn_clear_lst = [wwnn for wwnn in wwnn_lst if wwnn not in wwn_intersection]

    # zoning_aggregated_df['Wwn_type'] = zoning_aggregated_df.alias_member.apply(lambda wwn: wwn_check(wwn, wwnp_clear_lst, wwnn_clear_lst, wwn_intersection))

    zoning_aggregated_df = wwn_type(zoning_aggregated_df, portshow_aggregated_df)



    zoning_aggregated_df = zonemember_connection_verify(zoning_aggregated_df, portshow_aggregated_df)






    zoning_aggregated_df = lsan_state_verify(zoning_aggregated_df, switch_params_aggregated_df, fcrfabric_df, lsan_df)
    zoning_aggregated_df = zonemember_in_cfg_fabric_verify(zoning_aggregated_df)


     # zoning_aggregated_df['Member_in_cfg_Fabric'] = np.where(pd.isna(zoning_aggregated_df.zonemember_Fabric_name), np.nan, zoning_aggregated_df['Member_in_cfg_Fabric'])
    zoning_aggregated_df = drop_columns(zoning_aggregated_df)

    return zoning_aggregated_df 


def zoning_from_configuration(switch_params_aggregated_df, cfg_df, cfg_effective_df, zone_df, alias_df):

    cfg_join_df, cfg_effective_join_df, zone_join_df, alias_join_df = align_dataframe(switch_params_aggregated_df, cfg_df, cfg_effective_df, zone_df, alias_df)


    cfg_join_df = cfg_join_df.merge(cfg_effective_join_df, how='left', on=['Fabric_name', 'Fabric_label'])
    cfg_join_df['cfg_type'] = np.where(cfg_join_df.cfg == cfg_join_df.effective_config, 'effective', 'defined')

    cfg_join_df = cfg_join_df.reindex(columns = ['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type', 'zone'])
    zoning_aggregated_df = cfg_join_df.merge(zone_join_df, how='left', on= ['Fabric_name', 'Fabric_label', 'zone'])
    alias_join_df.rename(columns = {'alias': 'zone_member'}, inplace = True)
    zoning_aggregated_df = zoning_aggregated_df.merge(alias_join_df, how='left', on = ['Fabric_name', 'Fabric_label', 'zone_member'])
    zoning_aggregated_df.alias_member.fillna(zoning_aggregated_df.zone_member, inplace=True)

    return zoning_aggregated_df

def zonemember_connection_verify(zoning_aggregated_df, portshow_aggregated_df):

    port_node_name_df = portshow_aggregated_df[['PortName', 'NodeName']].copy()
    port_node_name_df.dropna(subset = ['PortName', 'NodeName'], inplace=True)
    port_node_name_df.rename(columns={'PortName': 'Strict_Wwnp', 'NodeName': 'alias_member'}, inplace=True )

    zoning_aggregated_df = zoning_aggregated_df.merge(port_node_name_df, how='left', on=['alias_member'])
    zoning_aggregated_df.Strict_Wwnp.fillna(zoning_aggregated_df.alias_member, inplace=True)

    port_columns_lst = ['Fabric_name', 'Fabric_label', 
                    'Device_Host_Name', 'Group_Name', 
                    'Device_type', 'deviceType', 'deviceSubtype', 'portType',
                    'PortName', 'NodeName', 'portId',
                    'chassis_name', 'switchName', 'Index_slot_port'
                    ]

    portcmd_join_df = portshow_aggregated_df.loc[:, port_columns_lst].copy()
    portcmd_join_df['Strict_Wwnp'] = portcmd_join_df.PortName
    portcmd_join_df = portcmd_join_df.rename(
        columns={'Fabric_name': 'zonemember_Fabric_name', 'Fabric_label': 'zonemember_Fabric_label'})
    portcmd_join_df.dropna(subset=['Strict_Wwnp'], inplace=True)
                                              
    zoning_aggregated_df = zoning_aggregated_df.merge(portcmd_join_df, how='left', on=['Strict_Wwnp']) 
    zoning_aggregated_df.drop(columns=['Strict_Wwnp'], inplace=True)

    return zoning_aggregated_df


def zonemember_in_cfg_fabric_verify(zoning_aggregated_df):

    zoning_aggregated_df['Member_in_cfg_Fabric'] = (zoning_aggregated_df['Fabric_name'] == zoning_aggregated_df['zonemember_Fabric_name']) & \
        (zoning_aggregated_df['Fabric_label'] == zoning_aggregated_df['zonemember_Fabric_label'])
    zoning_aggregated_df['Member_in_cfg_Fabric'] = zoning_aggregated_df['Member_in_cfg_Fabric'].where(pd.notna(zoning_aggregated_df.zonemember_Fabric_name), np.nan)
    zoning_aggregated_df['Member_in_cfg_Fabric'].replace(to_replace={1: 'Да', 0: 'Нет'}, inplace = True)
    mask_member_imported = zoning_aggregated_df['LSAN_device_state'].str.contains('Imported', na=False)
    mask_fabric_name = pd.notna(zoning_aggregated_df['zonemember_Fabric_name'])
    zoning_aggregated_df['Member_in_cfg_Fabric'] = np.where((mask_member_imported&mask_fabric_name), 'Да', zoning_aggregated_df['Member_in_cfg_Fabric'])

    return zoning_aggregated_df 

def lsan_state_verify(zoning_aggregated_df, switch_params_aggregated_df, fcrfabric_df, lsan_df):

    fcr_columns_lst = ['switchWwn', 'Fabric_name', 'Fabric_label']


    fcr_columns_dct = {'principal_switch_index': 'switch_index',	
                'principal_switchName': 'switchName',	
                'principal_switchWwn': 'switchWwn'}

    fcrfabric_join_df = fcrfabric_df.copy()
    fcrfabric_join_df.rename(columns={**fcr_columns_dct, **{'EX_port_switchWwn': 'Connected_switchWwn', 'EX_port_FID': 'Connected_Fabric_ID'}}, inplace=True)
    fcrfabric_join_df = dataframe_join(fcrfabric_join_df, switch_params_aggregated_df, fcr_columns_lst, 1)

    fcrfabric_lst=[
    'configname',
    'chassis_name',
    'chassis_wwn',
    'switchName',
    'switchWwn',
    'BB_Fabric_ID',
    'Fabric_name',
    'Fabric_label',
    'Connected_Fabric_ID',
    'Connected_Fabric_name',
    'Connected_Fabric_label'
    ]

    fcrfabric_join_df = fcrfabric_join_df.reindex(columns=fcrfabric_lst)
    fcrfabric_join_df.drop_duplicates(inplace=True)

    lsan_join_df, = align_dataframe(switch_params_aggregated_df, lsan_df, drop_columns=False)

    lsan_join_df = lsan_join_df.merge(fcrfabric_join_df, how='left', left_on=[*fcrfabric_lst[:8], 'Zone_Created_Fabric_ID'],
                                    right_on = [*fcrfabric_lst[:8], 'Connected_Fabric_ID'])

    lsan_columns_lst = [
        'zone',
        'zone_member',
        'LSAN_device_state',
        'Connected_Fabric_name',
        'Connected_Fabric_label']

    lsan_join_df = lsan_join_df.reindex(columns=lsan_columns_lst)
    lsan_join_df.rename(columns={'Connected_Fabric_name': 'Fabric_name', 
                                'Connected_Fabric_label': 'Fabric_label',
                                'zone_member': 'alias_member'}, inplace=True)
    lsan_join_columns_lst = ['Fabric_name', 'Fabric_label', 'zone', 'alias_member']
    zoning_aggregated_df = zoning_aggregated_df.merge(lsan_join_df, how='left', on=lsan_join_columns_lst) 

    return zoning_aggregated_df


def wwn_type(zoning_aggregated_df, portshow_aggregated_df):

    wwnn_lst = sorted(portshow_aggregated_df.NodeName.dropna().drop_duplicates().to_list())
    wwnp_lst = sorted(portshow_aggregated_df.PortName.dropna().drop_duplicates().to_list())
    wwn_intersection = set(wwnp_lst).intersection(wwnn_lst)
    wwnp_clear_lst = [wwnp for wwnp in wwnp_lst if wwnp not in wwn_intersection]
    wwnn_clear_lst = [wwnn for wwnn in wwnn_lst if wwnn not in wwn_intersection]
    
    def wwn_check(wwn, wwnp_lst, wwnn_lst, wwn_intersection):

        if wwn in wwn_intersection:
            return 'Wwnp'
        if wwn in wwnp_lst:
            return 'Wwnp'
        if wwn in wwnn_lst:
            return 'Wwnn'

        return np.nan

    zoning_aggregated_df['Wwn_type'] = zoning_aggregated_df.alias_member.apply(lambda wwn: wwn_check(wwn, wwnp_clear_lst, wwnn_clear_lst, wwn_intersection))

    return zoning_aggregated_df


def align_dataframe(switch_params_aggregated_df, *args, drop_columns=True):

    # add Fabric labels from switch_params_aggregated_df Fataframe
    # columns labels reqiured for join operation
    switchparams_lst = ['configname', 'chassis_name', 'chassis_wwn', 
                        'switchName', 'switchWwn', 
                        'Fabric_name', 'Fabric_label'
                        ]
    # create left DataFrame for join operation
    switchparams_aggregated_join_df = switch_params_aggregated_df.loc[:, switchparams_lst].copy()
    
    df_lst = []
    column_dct = {'principal_switch_index': 'switch_index',	
                'principal_switchName': 'switchName',	
                'principal_switchWwn': 'switchWwn'}

    for arg in args:
        df = arg.copy()
        df.rename(columns =  column_dct, inplace = True)

        df = df.merge(switchparams_aggregated_join_df, how = 'left', on = switchparams_lst[:5])
        
        df.dropna(subset = ['Fabric_name', 'Fabric_label'], inplace = True)
        if drop_columns:
            df.drop(columns = [*switchparams_lst[:5], 'switch_index', 'Fabric_ID'], inplace=True)
        df_lst.append(df)
        
    return df_lst


def drop_columns(zoning_aggregated_df):
    
    check_df = zoning_aggregated_df.copy()
    check_df.dropna(subset = ['zonemember_Fabric_name', 'zonemember_Fabric_label'], inplace=True)
    
    mask_fabricname = (check_df.Fabric_name == check_df.zonemember_Fabric_name).all()
    mask_fabriclabel = (check_df.Fabric_label == check_df.zonemember_Fabric_label).all()
    
    if mask_fabricname and mask_fabriclabel:
        zoning_aggregated_df.drop(columns=['zonemember_Fabric_name', 'zonemember_Fabric_label'], inplace=True)
        
    check_df.dropna(subset = ['Wwn_type'], inplace=True)
    mask_wwnp = (check_df.Wwn_type == 'Wwnp').all()
    if mask_wwnp:
        zoning_aggregated_df.drop(columns=['Wwn_type'], inplace=True)
        
    return zoning_aggregated_df

def same_subnet(series):
    
    if series[['zonemember_Fabric_name', 'zonemember_Fabric_label']].isna().any():
        return np.nan
    if series[['zonemember_Fabric_name', 'zonemember_Fabric_label']].equals(series[['Fabric_name', 'zonemember_Fabric_label']]):
        return 'Да'
    
    return 'Нет'