"""Module to create zoning configuration related DataFrames"""

import numpy as np
import pandas as pd
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
# import utilities.data_structure_operations as dsop
import utilities.module_execution as meop

from .report_zoning import zoning_report_main
from .zoning_aggregation import verify_cfg_type, zoning_aggregated
from .zoning_cfg_dashboard import cfg_dashborad
from .zoning_alias_dashboard import alias_dashboard
from .zoning_statistics import zonemember_statistics

# import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop



def zoning_analysis(switch_params_aggregated_df, portshow_aggregated_df, 
                            cfg_df, zone_df, alias_df, cfg_effective_df, 
                            fcrfabric_df, lsan_df, peerzone_df, 
                            report_creation_info_lst):
    """Main function to analyze zoning configuration"""
        
    # report_steps_dct contains current step desciption and force and export tags
    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    report_constant_lst, report_steps_dct, report_headers_df, report_columns_usage_dct = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['zoning_aggregated', 'alias_aggregated', 'portshow_zoned_aggregated', 
                    'zonemember_statistics', 'alias_statistics', 'effective_cfg_statistics', 
                    'Зонирование', 'Псевдонимы', 'Зонирование_AB', 'Порты_не_в_зонах', 'Порты_без_псевдономов', 
                    'Отсутствуют_в_сети', 'Статистика_зон', 'Статистика_псевдонимов', 'Статистика_конфигурации']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)
    # reade data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)

    # # unpacking DataFrames from the loaded list with data
    # # pylint: disable=unbalanced-tuple-unpacking
    # zoning_aggregated_df, alias_aggregated_df, zonemember_statistics_df, \
    #     portshow_zoned_aggregated_df, alias_statistics_df, effective_cfg_statistics_df, zoning_report_df, alias_report_df, \
    #         zoning_compare_report_df, unzoned_device_report_df, no_alias_device_report_df, zoning_absent_device_report_df,\
    #             zonemember_statistics_report_df,  alias_statistics_report_df, effective_cfg_statistics_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['cfg', 'cfg_effective', 'zone', 'alias', 'switch_params_aggregated', 
                            'switch_parameters', 'switchshow_ports', 'chassis_parameters', 
                            'portshow_aggregated', 'device_rename', 'report_columns_usage_upd', 
                            'portcmd', 'fdmi', 'nscamshow', 'nsshow', 'blade_servers', 'fabric_labels']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating zoning table'
        print(info, end =" ") 

        # aggregated DataFrames
        zoning_aggregated_df, alias_aggregated_df \
            = zoning_aggregated(switch_params_aggregated_df, portshow_aggregated_df, 
                                    cfg_df, zone_df, alias_df, cfg_effective_df, fcrfabric_df, lsan_df, peerzone_df, report_constant_lst)

        # create comprehensive statistics DataFrame with Fabric summaries and
        # zones statistics DataFrame without summaries  
        zonemember_statistics_df, zonemember_zonelevel_stat_df = zonemember_statistics(zoning_aggregated_df, report_constant_lst)
        # add zoning statistics notes, zone duplicates and zone pairs to zoning aggregated DataFrame
        zoning_aggregated_df = statistics_to_aggregated_zoning(zoning_aggregated_df, zonemember_zonelevel_stat_df)
        # check all fabric devices (Wwnp) for usage in zoning configuration
        portshow_zoned_aggregated_df = verify_cfg_type(portshow_aggregated_df, zoning_aggregated_df, ['PortName'])
        # create alias configuration statistics
        alias_statistics_df = alias_dashboard(alias_aggregated_df, portshow_zoned_aggregated_df)
        # create Effective zoning configuration summary statistics
        effective_cfg_statistics_df = cfg_dashborad(zonemember_statistics_df, portshow_zoned_aggregated_df, zoning_aggregated_df, alias_aggregated_df)
        # after finish display status
        meop.status_info('ok', max_title, len(info))

        # report tables
        zoning_report_df, alias_report_df, zoning_compare_report_df, \
            unzoned_device_report_df, no_alias_device_report_df, zoning_absent_device_report_df, \
                zonemember_statistics_report_df, alias_statistics_report_df, effective_cfg_statistics_report_df = \
                    zoning_report_main(zoning_aggregated_df, alias_aggregated_df, portshow_zoned_aggregated_df, 
                                        zonemember_statistics_df, alias_statistics_df, effective_cfg_statistics_df, 
                                        data_names, report_headers_df, report_columns_usage_dct)

        # create list with partitioned DataFrames
        data_lst = [zoning_aggregated_df, alias_aggregated_df, portshow_zoned_aggregated_df, 
                    zonemember_statistics_df, alias_statistics_df, effective_cfg_statistics_df, 
                    zoning_report_df, alias_report_df, zoning_compare_report_df, unzoned_device_report_df, 
                    no_alias_device_report_df, zoning_absent_device_report_df, zonemember_statistics_report_df, 
                    alias_statistics_report_df, effective_cfg_statistics_report_df]
        # saving data to json or csv file
        # save_data(report_constant_lst, data_names, *data_lst)
        # writing data to sql
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)  

    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        zoning_aggregated_df, alias_aggregated_df, portshow_zoned_aggregated_df, *_ = data_lst

    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):

        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)

    return zoning_aggregated_df, alias_aggregated_df, portshow_zoned_aggregated_df


def statistics_to_aggregated_zoning(zoning_aggregated_df, zonemember_zonelevel_stat_df):
    """
    Function to compliment zoning_aggregated_df DataFrame with 'Target_Initiator'and 'Target_model' notes 
    obtained from zone statistics DataFrame analysis
    """
    
    # create DataFrame with note columns only
    zone_columns = ['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type', 'zone']
    note_columns = ['Zone_name_device_names_ratio', 'Zone_name_device_names_related',
                    'zone_duplicated', 'zone_absorber', 'zone_paired',
                    'Zone_and_Pairzone_names_ratio', 'Zone_and_Pairzone_names_related',
                    'Pair_zone_note',
                    'Target_Initiator_note', 'Target_model_note', 'Mixed_zone_note', 'Effective_cfg_usage_note']
    note_columns = [column for column in note_columns if column in zonemember_zonelevel_stat_df.columns]
    zonenote_df = zonemember_zonelevel_stat_df.reindex(columns = [*zone_columns, *note_columns]).copy()
    # compliment aggregated DataFrame 
    zoning_aggregated_df = zoning_aggregated_df.merge(zonenote_df, how='left', on=zone_columns)
    return zoning_aggregated_df
