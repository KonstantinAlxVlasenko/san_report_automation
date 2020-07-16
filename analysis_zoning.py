"""Module to create zoning configuration related DataFrames"""

import pandas as pd
import numpy as np

from analysis_zoning_aggregation import zoning_aggregated
from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_dataframe import dataframe_segmentation


def zoning_analysis_main(switch_params_aggregated_df, portshow_aggregated_df, 
                            cfg_df, zone_df, alias_df, cfg_effective_df, fcrfabric_df, lsan_df,
                            report_columns_usage_dct, report_data_lst):
    """Main function to analyze zoning configuration"""
        
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['zoning_aggregated', 'alias_aggregated', 'zonemember_statistics', 'Зонирование', 'Псевдонимы']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    zoning_aggregated_df, alias_aggregated_df, zonemember_statistics_df, zoning_report_df, alias_report_df = data_lst

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

        # aggregated tables
        zoning_aggregated_df, alias_aggregated_df \
            = zoning_aggregated(switch_params_aggregated_df, portshow_aggregated_df, 
                                    cfg_df, zone_df, alias_df, cfg_effective_df, fcrfabric_df, lsan_df, report_data_lst)

        zonemember_statistics_df = zonemember_statistics(zoning_aggregated_df)
        # after finish display status
        status_info('ok', max_title, len(info))
        # report tables
        zoning_report_df = create_report(zoning_aggregated_df, data_names[3:4], report_columns_usage_dct, max_title)
        alias_report_df = create_report(alias_aggregated_df, data_names[4:], report_columns_usage_dct, max_title)
        # create list with partitioned DataFrames
        data_lst = [zoning_aggregated_df, alias_aggregated_df, zonemember_statistics_df, zoning_report_df, alias_report_df]
        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)

    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        zoning_aggregated_df, alias_aggregated_df, zonemember_statistics_df, zoning_report_df, alias_report_df \
            = verify_data(report_data_lst, data_names, *data_lst)
        data_lst = [zoning_aggregated_df, alias_aggregated_df, zonemember_statistics_df, zoning_report_df, alias_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return zoning_aggregated_df, alias_aggregated_df


def create_report(aggregated_df, data_name, report_columns_usage_dct, max_title):
    """
    Auxiliary function to remove unnecessary columns from aggregated DataFrame and
    extract required columns and create report dataframe
    """

    # pylint: disable=unbalanced-tuple-unpacking
    cleaned_df = drop_columns(aggregated_df, report_columns_usage_dct)
    report_df, = dataframe_segmentation(cleaned_df, data_name, report_columns_usage_dct, max_title)

    return report_df


def drop_columns(aggregated_df, report_columns_usage_dct):
    """Auxiliary function to drop unnecessary columns from aggreagated DataFrame"""
    
    fabric_name_usage = report_columns_usage_dct['fabric_name_usage']

    cleaned_df = aggregated_df.copy()
    # if Fabric name and Fabric labels are equal for config defined switch and zonemembers
    # then zonemember Fabric name and label columns are excessive
    check_df = cleaned_df.copy()
    check_df.dropna(subset = ['zonemember_Fabric_name', 'zonemember_Fabric_label'], inplace=True)
    mask_fabricname = (check_df.Fabric_name == check_df.zonemember_Fabric_name).all()
    mask_fabriclabel = (check_df.Fabric_label == check_df.zonemember_Fabric_label).all()
    if mask_fabricname and mask_fabriclabel:
        cleaned_df.drop(columns=['zonemember_Fabric_name', 'zonemember_Fabric_label'], inplace=True)
    # if zonemembers or aliases are not in the same fabric with the switch of zoning config definition
    # but there is only one fabric then zonemember_Fabric_name column is excessive
    if not fabric_name_usage and ('zonemember_Fabric_name' in cleaned_df.columns):
        cleaned_df.drop(columns=['zonemember_Fabric_name'], inplace=True)
    # if there is no LSAN devices in fabric then LSAN_device_state column should be dropped
    if cleaned_df.LSAN_device_state.isna().all():
        cleaned_df.drop(columns=['LSAN_device_state'], inplace=True)
    # if all zonemembers are defined through WWNp then Wwn_type column not informative
    check_df.dropna(subset = ['Wwn_type'], inplace=True)
    mask_wwnp = (check_df.Wwn_type == 'Wwnp').all()
    if mask_wwnp:
        cleaned_df.drop(columns=['Wwn_type'], inplace=True)
    # if only effective configuration present drop cfg_type column
    if cleaned_df['cfg_type'].str.contains('effective', na=False).all():
        cleaned_df.drop(columns=['cfg_type'], inplace=True)
    # if all zonemembers except not connected are in the same fabric 
    # with the cfg switch drop  Member_in_cfg_Fabric column
    if (cleaned_df['Member_in_cfg_Fabric'].dropna() == 'Да').all():
        cleaned_df.drop(columns=['Member_in_cfg_Fabric'], inplace=True)
        
    return cleaned_df


def zonemember_statistics(zoning_aggregated_df):

    zoning_modified_df = zoning_aggregated_df.copy()

    zoning_modified_df.deviceType.replace(to_replace={'BLADE_SRV': 'SRV'}, inplace=True)
    zoning_modified_df.deviceSubtype = zoning_modified_df['deviceType'] + ' ' + zoning_modified_df['deviceSubtype']

    mask_srv = zoning_modified_df.deviceType.str.contains('SRV', na=False)
    zoning_modified_df.deviceSubtype = np.where(mask_srv, np.nan, zoning_modified_df.deviceSubtype)

    def aggregataed_statistics(zoning_modified_df, zone=True):


        columns_lst = ['Fabric_device_status',
                    'deviceType',
                    'deviceSubtype',
                    'Device_type',
                    'Wwn_type'] 
        
        merge_lst = ['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type','zone']

        index_lst = [zoning_modified_df.Fabric_name, zoning_modified_df.Fabric_label,
                    zoning_modified_df.cfg, zoning_modified_df.cfg_type,
                    zoning_modified_df.zone]

        if not zone:
            index_lst = index_lst[:-1]
            merge_lst = merge_lst[:-1]

        zone_aggregated_statistics_df = pd.DataFrame()
        for column in columns_lst:
            column_statistics_df = pd.crosstab(index = index_lst,
                                        columns = zoning_modified_df[column],
                                        margins=True)
            if column == 'deviceType':
                column_statistics_df.rename(columns={'All': 'Total_fabric_devices'}, inplace=True)
            else:
                column_statistics_df.drop(columns=['All'], inplace=True)
                
            if column != 'Device_type':
                column_statistics_df.fillna(0, inplace=True)
                
            if zone_aggregated_statistics_df.empty:
                zone_aggregated_statistics_df = column_statistics_df.copy()
            else:
                zone_aggregated_statistics_df = zone_aggregated_statistics_df.merge(column_statistics_df, how='left', on=merge_lst)
                
                        
        return zone_aggregated_statistics_df
            


    zonemember_zonelevel_stat_df = aggregataed_statistics(zoning_modified_df)
    zonemember_fabriclevel_stat_df = aggregataed_statistics(zoning_modified_df, zone=False)

    zonemember_zonelevel_stat_df.reset_index(inplace=True)
    zonemember_zonelevel_stat_df.drop(zonemember_zonelevel_stat_df.index[zonemember_zonelevel_stat_df['Fabric_name'] == 'All'], inplace = True)
    zonemember_fabriclevel_stat_df.reset_index(inplace=True)

    zonemember_statistics_df = pd.concat([zonemember_zonelevel_stat_df, zonemember_fabriclevel_stat_df], ignore_index=True)



    zonemember_statistics_df['Total_fabric_devices'].fillna(0, inplace=True)

    online_columns = ['local', 'imported']
    online_columns_confirmed = [column for column in online_columns if column in zonemember_statistics_df.columns]
    zonemember_statistics_df['Total_connected_zonemembers'] = np.nan
    if len(online_columns_confirmed) == 1:
        zonemember_statistics_df['Total_connected_zonemembers'] = zonemember_statistics_df['Total_connected_zonemembers'].fillna(
            zonemember_statistics_df[online_columns_confirmed[0]])
    elif len(online_columns_confirmed) > 1:
        zonemember_statistics_df['Total_connected_zonemembers'] = zonemember_statistics_df[online_columns_confirmed[0]] + \
            zonemember_statistics_df[online_columns_confirmed[1]]


    return zonemember_statistics_df














