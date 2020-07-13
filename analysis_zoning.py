"""Module to create zoning configuration related DataFrames"""

import pandas as pd

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
    data_names = ['zoning_aggregated', 'alias_aggregated', 'Зонирование', 'Псевдонимы']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    zoning_aggregated_df, alias_aggregated_df, zoning_report_df, alias_report_df = data_lst

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

        # after finish display status
        status_info('ok', max_title, len(info))

        # report tables
        zoning_report_df = create_report(zoning_aggregated_df, data_names[2:3], report_columns_usage_dct, max_title)
        alias_report_df = create_report(alias_aggregated_df, data_names[3:], report_columns_usage_dct, max_title)

        # create list with partitioned DataFrames
        data_lst = [zoning_aggregated_df, alias_aggregated_df, zoning_report_df, alias_report_df]
        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        zoning_aggregated_df, alias_aggregated_df, zoning_report_df, alias_report_df = verify_data(report_data_lst, data_names, *data_lst)
        data_lst = [zoning_aggregated_df, alias_aggregated_df, zoning_report_df, alias_report_df]
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
    
    # if Fabric name and Fabric labels are equal for config defined switch and zonemembers
    # then zonemember Fabric name and label columns are excessive
    check_df = aggregated_df.copy()
    check_df.dropna(subset = ['zonemember_Fabric_name', 'zonemember_Fabric_label'], inplace=True)
    mask_fabricname = (check_df.Fabric_name == check_df.zonemember_Fabric_name).all()
    mask_fabriclabel = (check_df.Fabric_label == check_df.zonemember_Fabric_label).all()
    if mask_fabricname and mask_fabriclabel:
        aggregated_df.drop(columns=['zonemember_Fabric_name', 'zonemember_Fabric_label'], inplace=True)
    # if zonemembers or aliases are not in the same fabric with the switch of zoning config definition
    # but there is only one fabric then zonemember_Fabric_name column is excessive
    if not fabric_name_usage and ('zonemember_Fabric_name' in aggregated_df.columns):
        aggregated_df.drop(columns=['zonemember_Fabric_name'], inplace=True)
    # if there is no LSAN devices in fabric then LSAN_device_state column should be dropped
    if aggregated_df.LSAN_device_state.isna().all():
        aggregated_df.drop(columns=['LSAN_device_state'], inplace=True)
    # if all zonemembers are defined through WWNp then Wwn_type column not informative
    check_df.dropna(subset = ['Wwn_type'], inplace=True)
    mask_wwnp = (check_df.Wwn_type == 'Wwnp').all()
    if mask_wwnp:
        aggregated_df.drop(columns=['Wwn_type'], inplace=True)
        
    return aggregated_df

























