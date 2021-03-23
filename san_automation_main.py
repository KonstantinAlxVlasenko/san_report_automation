"""
Main module to run
Define folders where configuration data is stored
Define SAN Assessment project folder to store all data
Define customer name
File report_info_xlsx
"""

import os

import pandas as pd

from analysis_errdump import errdump_main
from analysis_sensor import sensor_analysis_main
from analysis_zoning import zoning_analysis_main
from analysis_err_sfp_cfg import err_sfp_cfg_analysis_main
from analysis_switch_params_upd import  switch_params_analysis_main
from analysis_blade_chassis import blademodule_analysis
from analysis_fabric_label import fabriclabels_main
from analysis_fabric_statistics import fabricstatistics_main
from analysis_isl import isl_main
from analysis_portcmd import portcmd_analysis_main
from analysis_switch_params import fabric_main
from collection_bladesystem import blade_system_extract
from collection_chassis_params import chassis_params_extract
from collection_fabric_membership import fabricshow_extract
from collection_fcrfabric_membership import fcr_extract
from collection_isl import interswitch_connection_extract
from collection_maps import maps_params_extract
from collection_nameserver import connected_devices_extract
from collection_logs import logs_extract
from collection_portcfg_sfp import portinfo_extract
from collection_portcmd import portcmdshow_extract
from collection_switch_params import switch_params_configshow_extract
from collection_zoning import zoning_extract
from collection_sensor import sensor_extract
from collection_synergy import synergy_system_extract
from collection_3par import storage_3par_extract
from common_operations_dataframe import list_to_dataframe
from common_operations_servicefile import (columns_import, dataframe_import,
                                           dct_from_columns,
                                           report_entry_values)
from parser_san_toolbox import (
    create_files_list_to_parse, create_parsed_dirs, santoolbox_process, find_max_title)


# global list with constant vars for report operations 
# (customer name, folder to save report, biggest file name (char number) to print info on screen)
# defined in config_preparation function
report_data_lst = []
# initial max filename title for status represenation
start_max_title = 60

# get report entry values from report file
customer_name, project_folder, ssave_folder, blade_folder, synergy_folder, local_3par_folder = report_entry_values(start_max_title)
max_title = find_max_title(ssave_folder)

print('\n\n')
info = f'ASSESSMENT FOR SAN SERVICE. CUSTOMER {customer_name}.'
print(info.center(max_title + 80, '.'))
print('\n')

# dictionary with report steps as keys. each keys has two values
# first value shows if it is required to export extracted data to excel table
# second value shows if it is required to initiate force data extraction if data have been already extracted
report_steps_dct = dct_from_columns('service_tables', max_title, \
    'keys', 'export_to_excel', 'force_extract', 'report_type', 'step_info', 'description', \
        init_file = 'report_info.xlsx', display_status=False)


def main():
    
    parsed_lst = config_preparation(customer_name, project_folder, ssave_folder, max_title)
    chassis_params_fabric_lst, chassis_params_df, maps_params_df = chassis_maps_params(parsed_lst)
    switch_params_lst, switch_params_df, switchshow_ports_df = switches_params(chassis_params_fabric_lst)
    fabricshow_df, ag_principal_df = switches_in_fabric(switch_params_lst)
    portshow_df, sfpshow_df, portcfgshow_df = ports_info_group(chassis_params_fabric_lst, switch_params_lst)    
    fdmi_df, nsshow_df, nscamshow_df = connected_devices(switch_params_lst)
    isl_df, trunk_df, porttrunkarea_df = interswitch_connection(switch_params_lst)
    fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df = fcrouting(switch_params_lst)
    cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df = zoning(switch_params_lst)
    sensor_df = sensor_readings(switch_params_lst)
    errdump_df = logs(chassis_params_fabric_lst)
    blade_module_df, blade_servers_df, blade_vc_df = blade_system(blade_folder)
    synergy_module_df, synergy_servers_df = synergy_system_extract(synergy_folder, report_data_lst)
    system_3par_df, port_3par_df, host_3par_df = storage_3par(nsshow_df, nscamshow_df, local_3par_folder, project_folder, report_data_lst)
    
    # set fabric names and labels
    fabricshow_ag_labels_df = fabriclabels_main(switchshow_ports_df, switch_params_df, fabricshow_df, ag_principal_df, report_data_lst)

    blade_module_loc_df = blademodule_analysis(blade_module_df, synergy_module_df, report_data_lst)

    # switch_params_aggregated_df, report_columns_usage_dct, fabric_labeled_df = \
    #         fabric_main(fabricshow_ag_labels_df, chassis_params_df, switch_params_df, maps_params_df, report_data_lst)

    report_columns_usage_dct, switch_params_aggregated_df, fabric_clean_df = \
            switch_params_analysis_main(fabricshow_ag_labels_df, chassis_params_df, switch_params_df, maps_params_df, blade_module_loc_df, ag_principal_df, report_data_lst)

    isl_aggregated_df, isl_statistics_df = \
        isl_main(fabricshow_ag_labels_df, switch_params_aggregated_df, report_columns_usage_dct, 
    isl_df, trunk_df, fcredge_df, portshow_df, sfpshow_df, portcfgshow_df, switchshow_ports_df, report_data_lst)

    portshow_aggregated_df = \
        portcmd_analysis_main(portshow_df, switchshow_ports_df, switch_params_df, switch_params_aggregated_df, isl_aggregated_df, nsshow_df, nscamshow_df, ag_principal_df, \
            alias_df, fdmi_df, blade_module_df, blade_servers_df, blade_vc_df, synergy_module_df, synergy_servers_df, report_columns_usage_dct, report_data_lst)

    portshow_sfp_aggregated_df =  err_sfp_cfg_analysis_main(portshow_aggregated_df, sfpshow_df, portcfgshow_df, report_columns_usage_dct, report_data_lst)

    zoning_aggregated_df, alias_aggregated_df, portshow_zoned_aggregated_df = \
        zoning_analysis_main(switch_params_aggregated_df, portshow_aggregated_df, 
                                cfg_df, zone_df, alias_df, cfg_effective_df, 
                                    fcrfabric_df, lsan_df, peerzone_df,
                                        report_columns_usage_dct, report_data_lst)

    sensor_aggregated_df = sensor_analysis_main(sensor_df, switch_params_aggregated_df, report_columns_usage_dct, report_data_lst)

    fabric_statistics_df = \
        fabricstatistics_main(portshow_aggregated_df, switchshow_ports_df, fabricshow_ag_labels_df, 
                                nscamshow_df, portshow_df, report_columns_usage_dct, report_data_lst)

    errdump_aggregated_df, raslog_counter_df = \
        errdump_main(errdump_df, switchshow_ports_df, switch_params_aggregated_df, portshow_aggregated_df, report_columns_usage_dct, report_data_lst)
      

def config_preparation(customer_name, project_folder, ssave_folder, max_title):
    
    global report_data_lst
    
    # max_title = find_max_title(ssave_folder)

    # create directories in SAN Assessment project folder 
    dir_parsed_sshow, dir_parsed_others, dir_report, dir_data_objects = create_parsed_dirs(customer_name, project_folder, max_title)

    # check for switches unparsed configuration data
    # returns list with config data files and maximum config filename length
    unparsed_lst = create_files_list_to_parse(ssave_folder, max_title)
    
    # creates list with report data
    report_data_lst = [customer_name, dir_report, dir_data_objects, max_title, report_steps_dct]
    
    # export unparsed config filenames to DataFrame and saves it to excel file
    list_to_dataframe(unparsed_lst, report_data_lst, 'unparsed_files', columns = ['sshow', 'amsmaps'])

    # returns list with parsed data
    parsed_lst, parsed_filenames_lst = santoolbox_process(unparsed_lst, dir_parsed_sshow, dir_parsed_others, max_title)
    # export parsed config filenames to DataFrame and saves it to excel file
    parsed_lst_columns = ['chassis_name', 'sshow_config', 'ams_maps_log_config']
    list_to_dataframe(parsed_filenames_lst, report_data_lst, 'parsed_files', columns = parsed_lst_columns) 

    return parsed_lst


def chassis_maps_params(parsed_lst):

    # check configuration files to extract chassis parameters and save it to the list of lists
    chassis_params_fabric_lst = chassis_params_extract(parsed_lst, report_data_lst)
    # export chassis parameters list to DataFrame and saves it to excel file
    chassis_params_fabric_df = list_to_dataframe(chassis_params_fabric_lst, report_data_lst, 'chassis_parameters', 'chassis')
    maps_params_fabric_lst = maps_params_extract(parsed_lst, report_data_lst)
    # export maps parameters list to DataFrame and saves it to excel file
    maps_params_fabric_df = list_to_dataframe(maps_params_fabric_lst, report_data_lst, 'maps_parameters', 'maps')
    
    return chassis_params_fabric_lst, chassis_params_fabric_df, maps_params_fabric_df


def switches_params(chassis_params_fabric_lst):
     
    switch_params_lst, switchshow_ports_lst = switch_params_configshow_extract(chassis_params_fabric_lst, report_data_lst)
    switch_params_df = list_to_dataframe(switch_params_lst, report_data_lst, 'switch_parameters', 'switch')
    switchshow_ports_df = list_to_dataframe(switchshow_ports_lst, report_data_lst, 'switchshow_ports', 'switch', columns_title_import = 'switchshow_portinfo_columns')
    
    return switch_params_lst, switch_params_df, switchshow_ports_df
        

def switches_in_fabric(switch_params_lst):
    fabricshow_lst, ag_principal_lst = fabricshow_extract(switch_params_lst, report_data_lst)
    # export fabrichow and fcrfabricshow lists to DataFrame and saves it to excel file
    fabricshow_df = list_to_dataframe(fabricshow_lst, report_data_lst, 'fabricshow', 'fabricshow')
    ag_principal_df = list_to_dataframe(ag_principal_lst, report_data_lst, 'ag_principal', 'fabricshow', columns_title_import = 'ag_columns')

    return fabricshow_df, ag_principal_df


def ports_info_group(chassis_params_fabric_lst, switch_params_lst):        
    portshow_lst = portcmdshow_extract(chassis_params_fabric_lst, report_data_lst)
    portshow_df = list_to_dataframe(portshow_lst, report_data_lst, 'portcmd', 'portcmd')
        
    sfpshow_lst, portcfgshow_lst = portinfo_extract(switch_params_lst, report_data_lst)
    sfpshow_df = list_to_dataframe(sfpshow_lst, report_data_lst, 'sfpshow', 'portinfo')
    portcfgshow_df = list_to_dataframe(portcfgshow_lst, report_data_lst, 'portcfgshow', 'portinfo', columns_title_import = 'portcfg_columns')
    
    return portshow_df, sfpshow_df, portcfgshow_df
    
    
def connected_devices(switch_params_lst):
    fdmi_lst, nsshow_lst, nscamshow_lst = connected_devices_extract(switch_params_lst, report_data_lst)
    fdmi_df = list_to_dataframe(fdmi_lst, report_data_lst, 'fdmi', 'connected_dev')
    nsshow_df = list_to_dataframe(nsshow_lst, report_data_lst, 'nsshow', 'connected_dev', columns_title_import = 'nsshow_columns')
    nscamshow_df = list_to_dataframe(nscamshow_lst, report_data_lst, 'nscamshow', 'connected_dev', columns_title_import = 'nsshow_columns')
    
    return fdmi_df, nsshow_df, nscamshow_df
    
    
def interswitch_connection(switch_params_lst):
    isl_lst, trunk_lst, porttrunkarea_lst = interswitch_connection_extract(switch_params_lst, report_data_lst)
    isl_df = list_to_dataframe(isl_lst, report_data_lst, 'isl', 'isl')
    trunk_df = list_to_dataframe(trunk_lst, report_data_lst, 'trunk', 'isl', columns_title_import = 'trunk_columns')
    porttrunkarea_df = list_to_dataframe(porttrunkarea_lst, report_data_lst, 'porttrunkarea', 'isl', columns_title_import = 'porttrunkarea_columns')
    
    return isl_df, trunk_df, porttrunkarea_df
    
    
def fcrouting(switch_params_lst):    
    fcrfabric_lst, fcrproxydev_lst, fcrphydev_lst, lsan_lst, fcredge_lst, fcrresource_lst = fcr_extract(switch_params_lst, report_data_lst)
    fcrfabric_df = list_to_dataframe(fcrfabric_lst, report_data_lst, 'fcrfabric', 'fcr', columns_title_import = 'fcrfabric_columns')
    fcrproxydev_df = list_to_dataframe(fcrproxydev_lst, report_data_lst, 'fcrproxydev', 'fcr', columns_title_import = 'fcrproxydev_columns')
    fcrphydev_df =list_to_dataframe(fcrphydev_lst, report_data_lst, 'fcrphydev', 'fcr', columns_title_import = 'fcrphydev_columns')
    lsan_df = list_to_dataframe(lsan_lst, report_data_lst, 'lsan', 'fcr', columns_title_import = 'lsan_columns') 
    fcredge_df = list_to_dataframe(fcredge_lst, report_data_lst, 'fcredge', 'fcr', columns_title_import = 'fcredge_columns')       
    fcrresource_df = list_to_dataframe(fcrresource_lst, report_data_lst, 'fcrresource', 'fcr', columns_title_import = 'fcrresource_columns')
    
    return fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df
    
def zoning(switch_params_lst):
    cfg_lst, zone_lst, alias_lst, cfg_effective_lst, zone_effective_lst, \
        peerzone_lst, peerzone_effective_lst = zoning_extract(switch_params_lst, report_data_lst)
    cfg_df = list_to_dataframe(cfg_lst, report_data_lst, 'cfg', 'zoning')
    zone_df = list_to_dataframe(zone_lst, report_data_lst, 'zone', 'zoning', columns_title_import = 'zone_columns')
    alias_df = list_to_dataframe(alias_lst, report_data_lst, 'alias', 'zoning', columns_title_import = 'alias_columns')
    cfg_effective_df = list_to_dataframe(cfg_effective_lst, report_data_lst, 'cfg_effective', 'zoning', columns_title_import = 'cfg_effective_columns')
    zone_effective_df = list_to_dataframe(zone_effective_lst, report_data_lst, 'zone_effective', 'zoning', columns_title_import = 'zone_effective_columns')
    peerzone_df = list_to_dataframe(peerzone_lst, report_data_lst, 'peerzone', 'zoning', columns_title_import = 'peerzone_columns')
    peerzone_effective_df = list_to_dataframe(peerzone_effective_lst, report_data_lst, 'peerzone_effective', 'zoning', columns_title_import = 'peerzone_effective_columns')
    
    return cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df       
          

def sensor_readings(switch_params_lst):
    sensor_lst = sensor_extract(switch_params_lst, report_data_lst)
    sensor_df = list_to_dataframe(sensor_lst, report_data_lst, 'sensor', 'sensor')

    return sensor_df


def logs(chassis_params_fabric_lst):
    errdump_lst = logs_extract(chassis_params_fabric_lst, report_data_lst)
    errdump_df = list_to_dataframe(errdump_lst, report_data_lst, 'errdump', 'log')

    return errdump_df


def blade_system(blade_folder):
    module_comprehensive_lst, blades_comprehensive_lst, blade_vc_comprehensive_lst = blade_system_extract(blade_folder, report_data_lst)
    blade_module_df = list_to_dataframe(module_comprehensive_lst, report_data_lst, 'blade_interconnect', 'blades')
    blade_servers_df = list_to_dataframe(blades_comprehensive_lst, report_data_lst, 'blade_servers', 'blades', columns_title_import = 'blade_columns')
    blade_vc_df = list_to_dataframe(blade_vc_comprehensive_lst, report_data_lst, 'blade_vc', 'blades', columns_title_import = 'blade_vc_columns')

    return blade_module_df, blade_servers_df, blade_vc_df


def storage_3par(nsshow_df, nscamshow_df, local_3par_folder, project_folder, report_data_lst):
    system_3par_comprehensive_lst, port_3par_comprehensive_lst, host_3par_comprehensive_lst \
        = storage_3par_extract(nsshow_df, nscamshow_df, local_3par_folder, project_folder, report_data_lst)
    system_3par_df = list_to_dataframe(system_3par_comprehensive_lst, report_data_lst, 'system_3par', '3par')
    port_3par_df = list_to_dataframe(port_3par_comprehensive_lst, report_data_lst, 'port_3par', '3par', columns_title_import = 'port_columns')
    host_3par_df = list_to_dataframe(host_3par_comprehensive_lst, report_data_lst, 'host_3par', '3par', columns_title_import = 'host_columns')

    return system_3par_df, port_3par_df, host_3par_df


if __name__ == "__main__":
    main()
    # config_data, report_data_lst  = config_data_check()
    # switch_params_check(config_data, report_data_lst)
    
    
    
    
    print('\nExecution successfully finished\n')
