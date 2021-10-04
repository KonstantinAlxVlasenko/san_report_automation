"""
Main module to run
Define folders where configuration data is stored
Define SAN Assessment project folder to store all data
Define customer name
File report_info_xlsx
"""

import san_analysis
import san_init
import san_parser
# from common_operations_table_report import report_format_completion

from utilities import report_format_completion

# import pandas as pd


# from analysis_errdump import errdump_main
# from analysis_sensor import sensor_analysis_main
# from analysis_zoning import zoning_analysis_main
# from analysis_err_sfp_cfg import err_sfp_cfg_analysis_main
# from analysis_switch_params_upd import  switch_params_analysis_main
# from analysis_blade_chassis import blademodule_analysis
# from analysis_fabric_label import fabriclabels_main
# from analysis_fabric_statistics import fabricstatistics_main
# from analysis_isl import isl_main
# from analysis_portcmd import portcmd_analysis_main
# from analysis_storage_host import storage_host_analysis_main
# from analysis_maps_npiv_ports import maps_npiv_ports_main
# from common_operations_dataframe import list_to_dataframe
# from common_operations_servicefile import (columns_import, dataframe_import,
#                                            dct_from_columns,
#                                            report_entry_values)
# from common_operations_table_report import dataframe_to_report
# from parser_san_toolbox import (
#     create_files_list_to_parse, create_parsed_dirs, santoolbox_process, find_max_title)




# global list with constants for report operations 
# # (customer name, project directory, database directory and biggest file name (char number) to print info on screen)
# # defined in config_preparation function
# report_constant_lst = []
# # report_columns_usage_dct = {}


# # initial max filename title for status represenation
# start_max_title = 60

# # get report entry values from report file
# customer_name, project_folder, ssave_folder, blade_folder, synergy_folder, local_3par_folder = report_entry_values(start_max_title)
# max_title = find_max_title(ssave_folder)

# print('\n\n')
# info = f'ASSESSMENT FOR SAN SERVICE. CUSTOMER {customer_name}.'
# print(info.center(max_title + 80, '.'))
# print('\n')

# # dictionary with data titles as keys. each keys has two values
# # first value shows if it is required to export extracted data to excel table
# # second value shows if it is required to initiate force data extraction if data have been already extracted
# report_steps_dct = dct_from_columns('service_tables', max_title, \
#     'keys', 'export_to_excel', 'force_extract', 'report_type', 'step_info', 'description', \
#         init_file = 'report_info.xlsx', display_status=False)

# project_steps_df = dataframe_import('service_tables', max_title, init_file = 'report_info.xlsx', display_status=False)
# numeric_columns = ['export_to_excel', 'force_extract', 'sort_weight']
# project_steps_df[numeric_columns] = project_steps_df[numeric_columns].apply(pd.to_numeric, errors='ignore')

# # Data_frame with report columns
# report_headers_df = dataframe_import('customer_report', max_title, display_status=False)

# report_creation_info_lst = [report_constant_lst, report_steps_dct, report_headers_df]

def main():


    report_entry_sr, report_creation_info_lst, project_steps_df, software_path_df = san_init.service_initialization()
    # parsed_lst = config_preparation(customer_name, project_folder, ssave_folder, max_title)

    parsed_sshow_maps_lst = san_init.switch_config_preprocessing(report_entry_sr, report_creation_info_lst, software_path_df)

    extracted_configuration_lst = san_parser.system_configuration_extract(parsed_sshow_maps_lst, report_entry_sr, report_creation_info_lst)

    san_analysis.system_configuration_analysis(extracted_configuration_lst, report_creation_info_lst)

    # # collection
    # # chassis parameters parsing
    # chassis_params_df = sp.chassis_params_extract(parsed_lst, report_creation_info_lst)
    # # maps parameters parsing
    # maps_params_df = sp.maps_params_extract(parsed_lst, report_creation_info_lst)
    # # switch parameters parsing
    # switch_params_df, switchshow_ports_df = sp.switch_params_extract(chassis_params_df, report_creation_info_lst)
    # # fabric membership pasing (AG swithe information extracted from Principal switches)
    # fabricshow_df, ag_principal_df = sp.fabric_membership_extract(switch_params_df, report_creation_info_lst)
    # # portshow statistics parsing
    # portshow_df = sp.portcmd_extract(chassis_params_df, report_creation_info_lst)
    # # port sfp and cfg parsing
    # sfpshow_df, portcfgshow_df = sp.portcfg_sfp_extract(switch_params_df, report_creation_info_lst)
    # # nameserver parsing
    # fdmi_df, nsshow_df, nscamshow_df = sp.connected_devices_extract(switch_params_df, report_creation_info_lst)
    # # inter switch connection parsing
    # isl_df, trunk_df, porttrunkarea_df, lsdb_df = sp.interswitch_connection_extract(switch_params_df, report_creation_info_lst)
    # # fabric routing parsing
    # fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df = sp.fcr_membership_extract(switch_params_df, report_creation_info_lst)
    # # zoning configuration parsing
    # cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df = \
    #     sp.zoning_extract(switch_params_df, report_creation_info_lst)
    # # switch sensors parsing
    # sensor_df = sp.sensor_extract(chassis_params_df, report_creation_info_lst)
    # # error log parsing
    # errdump_df = sp.log_extract(chassis_params_df, report_creation_info_lst)
    # # blade system configuration parsing
    # blade_module_df, blade_servers_df, blade_vc_df = sp.blade_system_extract(report_entry_sr, report_creation_info_lst)
    # # synergy system configuration parsing
    # synergy_module_df, synergy_servers_df = sp.synergy_system_extract(report_entry_sr, report_creation_info_lst)
    # # 3PAR storage system configuration download and parsing
    # system_3par_df, port_3par_df, host_3par_df = \
    #         sp.storage_3par_extract(nsshow_df, nscamshow_df, report_entry_sr, report_creation_info_lst)


    # chassis_params_df, maps_params_df, \
    #     switch_params_df, switchshow_ports_df,\
    #         fabricshow_df, ag_principal_df, \
    #             portshow_df, sfpshow_df, portcfgshow_df,\
    #                 fdmi_df, nsshow_df, nscamshow_df,\
    #                     isl_df, trunk_df, porttrunkarea_df, lsdb_df,\
    #                         fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df,\
    #                             cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df,\
    #                                 sensor_df, errdump_df,\
    #                                     blade_module_df, blade_servers_df, blade_vc_df,\
    #                                         synergy_module_df, synergy_servers_df,\
    #                                             system_3par_df, port_3par_df, host_3par_df = extracted_configuration_lst
    
    # # analysis
    # # set fabric names and labels
    # fabricshow_ag_labels_df = \
    #     sa.fabric_label_analysis(switchshow_ports_df, switch_params_df, fabricshow_df, ag_principal_df, report_creation_info_lst)
    # blade_module_loc_df = sa.blade_system_analysis(blade_module_df, synergy_module_df, report_creation_info_lst)
    # report_columns_usage_dct, switch_params_aggregated_df, fabric_clean_df = \
    #         sa.switch_params_analysis(fabricshow_ag_labels_df, chassis_params_df, switch_params_df, maps_params_df, blade_module_loc_df, ag_principal_df, report_creation_info_lst)

    # if len(report_creation_info_lst) == 3:
    #     report_creation_info_lst.append(report_columns_usage_dct)

    # isl_aggregated_df, isl_statistics_df = \
    #     sa.isl_analysis(fabricshow_ag_labels_df, switch_params_aggregated_df, isl_df, trunk_df, lsdb_df, 
    #                         fcredge_df, portshow_df, sfpshow_df, portcfgshow_df, switchshow_ports_df, report_creation_info_lst)

    # portshow_aggregated_df = \
    #     sa.portcmd_analysis(portshow_df, switchshow_ports_df, switch_params_df, switch_params_aggregated_df, isl_aggregated_df, 
    #                             nsshow_df, nscamshow_df, ag_principal_df, porttrunkarea_df, alias_df, fdmi_df, blade_module_df, 
    #                             blade_servers_df, blade_vc_df, synergy_module_df, synergy_servers_df, 
    #                             system_3par_df, port_3par_df, report_creation_info_lst)


    # portshow_sfp_aggregated_df =  sa.port_err_sfp_cfg_analysis(portshow_aggregated_df, switch_params_aggregated_df, 
    #                                                             sfpshow_df, portcfgshow_df, isl_statistics_df, report_creation_info_lst)

    
    # portshow_npiv_df = sa.maps_npiv_ports_analysis(portshow_sfp_aggregated_df, switch_params_aggregated_df, isl_statistics_df, report_creation_info_lst)
    
    # zoning_aggregated_df, alias_aggregated_df, portshow_zoned_aggregated_df = \
    #     sa.zoning_analysis(switch_params_aggregated_df, portshow_aggregated_df, cfg_df, zone_df, alias_df, 
    #                         cfg_effective_df, fcrfabric_df, lsan_df, peerzone_df, report_creation_info_lst)


    # storage_host_aggregated_df = sa.storage_host_analysis(host_3par_df, system_3par_df, port_3par_df, 
    #                                                         portshow_aggregated_df, zoning_aggregated_df, report_creation_info_lst)

    # sensor_aggregated_df = sa.sensor_analysis(sensor_df, switch_params_aggregated_df, report_creation_info_lst)

    # fabric_port_statistics_df = sa.fabric_port_statistics_analysis(portshow_aggregated_df, switchshow_ports_df, fabricshow_ag_labels_df, 
    #                                                             nscamshow_df, portshow_df, report_creation_info_lst)
    

    # errdump_aggregated_df, raslog_counter_df = \
    #     sa.errdump_analysis(errdump_df, switchshow_ports_df, switch_params_aggregated_df, portshow_aggregated_df, report_creation_info_lst)



    report_format_completion(project_steps_df, report_creation_info_lst)
    print('\nExecution successfully finished\n')
      

# def config_preparation(customer_name, project_folder, ssave_folder, max_title):
    
#     global report_constant_lst
    
#     # max_title = find_max_title(ssave_folder)

#     # create directories in SAN Assessment project folder 
#     dir_parsed_sshow, dir_parsed_others, dir_report, dir_database = create_parsed_dirs(customer_name, project_folder, max_title)

#     # check for switches unparsed configuration data
#     # returns list with config data files and maximum config filename length
#     unparsed_lst = create_files_list_to_parse(ssave_folder, max_title)
    
#     # creates list with report data
#     report_constant_lst.extend([customer_name, dir_report, dir_database, max_title])
    
#     # export unparsed config filenames to DataFrame and saves it to excel file
#     unparsed_ssave_df = list_to_dataframe(unparsed_lst, max_title, columns = ['sshow', 'amsmaps'])
#     dataframe_to_report(unparsed_ssave_df, 'unparsed_files', report_creation_info_lst)
#     # returns list with parsed data
#     parsed_lst, parsed_filenames_lst = santoolbox_process(unparsed_lst, dir_parsed_sshow, dir_parsed_others, max_title)
#     # export parsed config filenames to DataFrame and saves it to excel file
#     parsed_lst_columns = ['chassis_name', 'sshow_config', 'ams_maps_log_config']
#     parsed_ssave_df = list_to_dataframe(parsed_filenames_lst, max_title, columns=parsed_lst_columns) 
#     dataframe_to_report(parsed_ssave_df, 'parsed_files', report_creation_info_lst)
#     return parsed_lst



if __name__ == "__main__":
    main()
    
