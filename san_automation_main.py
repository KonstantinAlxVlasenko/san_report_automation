import pandas as pd
import os
from files_operations import export_lst_to_excel, columns_import, dct_from_columns, dataframe_import
from san_toolbox_parser import create_parsed_dirs, create_files_list_to_parse, santoolbox_process
from san_chassis_params import chassis_params_extract
from san_switch_params import switch_params_configshow_extract
from san_amsmaps_log import maps_params_extract
from san_fabrics import fabricshow_extract
from san_portcmdshow import portcmdshow_extract
from san_portinfo import portinfo_extract
from san_connected_devices import connected_devices_extract
from san_isl import interswitch_connection_extract
from san_fcr import fcr_extract
from san_zoning import zoning_extract
from san_fabrics_labels import fabriclabels_main
from san_fabric_statistic import fabricstatistics_main
from san_switch_report_tables import fabric_main
from san_isl_report_tables import isl_main
from dataframe_operations import report_entry_values

"""
Main module to run
Define folders where configuration data is stored
Define SAN Assessment project folder to store all data
Define customer name
File report_info_xlsx
"""


print('\n\n')

# global list with constant vars for report operations 
# (customer name, folder to save report, biggest file name (char number) to print info on screen)
report_data_lst = []
# initial max filename title for status represenation
start_max_title = 60

# report_info_dct = dct_from_columns('report', start_max_title, 'name', 'value', init_file = 'report_info.xlsx')

# customer_name = report_info_dct['customer_name']
# project_folder = os.path.normpath(report_info_dct['project_folder'])
# ssave_folder = os.path.normpath(report_info_dct['supportsave_folder'])

# get report entry from report file
customer_name, project_folder, ssave_folder = report_entry_values(start_max_title)

# report_entry_df = dataframe_import('report', start_max_title, 'report_info.xlsx', ['name', 'value'], 'name')
# customer_name = report_entry_df.loc['customer_name', 'value']
# project_folder = os.path.normpath(report_entry_df.loc['project_folder', 'value'])
# ssave_folder = os.path.normpath(report_entry_df.loc['supportsave_folder', 'value'])

# list with extracted customer name, supportsave folder and project folder
# report_info_lst = columns_import('report', start_max_title, 'report_data', init_file = 'report_info.xlsx') 
# customer_name = rf'{report_info_lst[0]}'
# project_folder = rf'{report_info_lst[1]}'
# ssave_folder = rf'{report_info_lst[2]}'


# dictionary with report steps as keys. each keys has two values
# first value shows if it is required to export extracted data to excel table
# second value shows if it is required to initiate force data extraction if data have been already extracted
report_steps_dct = dct_from_columns('service_tables', start_max_title, 'keys', 'export_to_excel', 'force_extract', init_file = 'report_info.xlsx')


print('\n\n')
info = f'ASSESSMENT FOR SAN {customer_name}'
print(info.center(start_max_title + 80, '.'))


def main():
    
    parsed_lst = config_preparation()
    chassis_params_fabric_lst, chassis_params_df, maps_params_df = chassis_maps_params(parsed_lst)
    switch_params_lst, switch_params_df, switchshow_ports_df = switches_params(chassis_params_fabric_lst)
    fabricshow_df, ag_principal_df = switches_in_fabric(switch_params_lst)
    portshow_df, sfpshow_df, portcfgshow_df = ports_info_group(chassis_params_fabric_lst, switch_params_lst)    
    fdmi_df, nsshow_df, nscamshow_df = connected_devices(switch_params_lst)
    isl_df, trunk_df, porttrunkarea_df = interswitch_connection(switch_params_lst)
    fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df = fcrouting(switch_params_lst)
    cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df = zoning(switch_params_lst)
    
    # set fabric names and labels
    fabricshow_ag_labels_df = fabriclabels_main(switchshow_ports_df, fabricshow_df, ag_principal_df, report_data_lst)

    switch_params_aggregated_df, report_columns_usage_dct, fabric_labeled_df, switches_report_df, fabric_report_df, \
        global_fabric_parameters_report_df, switches_parameters_report_df, licenses_report_df = \
            fabric_main(fabricshow_ag_labels_df, chassis_params_df, switch_params_df, maps_params_df, report_data_lst)


    fabric_statistics_df, fabric_statistics_summary_df = \
        fabricstatistics_main(report_columns_usage_dct, switchshow_ports_df, 
        fabricshow_ag_labels_df, nscamshow_df, portshow_df, report_data_lst)

    fabric_clean_df, isl_report_df, ifl_report_df = \
        isl_main(fabricshow_ag_labels_df, switch_params_aggregated_df, report_columns_usage_dct, 
    isl_df, trunk_df, fcredge_df, sfpshow_df, portcfgshow_df, switchshow_ports_df, report_data_lst)
      

def config_preparation():
    
    global report_data_lst
    
    # create directories in SAN Assessment project folder 
    dir_parsed_sshow, dir_parsed_others, dir_report, dir_data_objects = create_parsed_dirs(customer_name, project_folder, start_max_title)

    # check for switches unparsed configuration data
    # returns list with config data files and maximum config filename length
    unparsed_lst, max_title = create_files_list_to_parse(ssave_folder)
    
    # creates list with report data
    report_data_lst = [customer_name, dir_report, dir_data_objects, max_title, report_steps_dct]
    
    # export unparsed config filenames to DataFrame and saves it to excel file
    export_lst_to_excel(unparsed_lst, report_data_lst, 'unparsed_files', columns = ['sshow', 'amsmaps'])

    # returns list with parsed data
    parsed_lst, parsed_filenames_lst = santoolbox_process(unparsed_lst, dir_parsed_sshow, dir_parsed_others, max_title)
    # export parsed config filenames to DataFrame and saves it to excel file
    parsed_lst_columns = ['chassis_name', 'sshow_config', 'ams_maps_log_config']
    export_lst_to_excel(parsed_filenames_lst, report_data_lst, 'parsed_files', columns = parsed_lst_columns) 

    return parsed_lst #, report_data_lst


def chassis_maps_params(parsed_lst):

    # check configuration files to extract chassis parameters and save it to the list of lists
    chassis_params_fabric_lst = chassis_params_extract(parsed_lst, report_data_lst)
    # export chassis parameters list to DataFrame and saves it to excel file
    chassis_params_fabric_df = export_lst_to_excel(chassis_params_fabric_lst, report_data_lst, 'chassis_parameters', 'chassis')
    
    maps_params_fabric_lst = maps_params_extract(parsed_lst, report_data_lst)
    # export maps parameters list to DataFrame and saves it to excel file
    maps_params_fabric_df = export_lst_to_excel(maps_params_fabric_lst, report_data_lst, 'maps_parameters', 'maps')
    
    return chassis_params_fabric_lst, chassis_params_fabric_df, maps_params_fabric_df


def switches_params(chassis_params_fabric_lst):
     
    switch_params_lst, switchshow_ports_lst = switch_params_configshow_extract(chassis_params_fabric_lst, report_data_lst)
    switch_params_df = export_lst_to_excel(switch_params_lst, report_data_lst, 'switch_parameters', 'switch')
    switchshow_ports_df = export_lst_to_excel(switchshow_ports_lst, report_data_lst, 'switchshow_ports', 'switch', columns_title_import = 'switchshow_portinfo_columns')
    
    return switch_params_lst, switch_params_df, switchshow_ports_df
        

def switches_in_fabric(switch_params_lst):
    fabricshow_lst, ag_principal_lst = fabricshow_extract(switch_params_lst, report_data_lst)
    # export fabrichow and fcrfabricshow lists to DataFrame and saves it to excel file
    fabricshow_df = export_lst_to_excel(fabricshow_lst, report_data_lst, 'fabricshow', 'fabricshow')
    ag_principal_df = export_lst_to_excel(ag_principal_lst, report_data_lst, 'ag_principal', 'fabricshow', columns_title_import = 'ag_columns')

    return fabricshow_df, ag_principal_df


def ports_info_group(chassis_params_fabric_lst, switch_params_lst):        
    portshow_lst = portcmdshow_extract(chassis_params_fabric_lst, report_data_lst)
    portshow_df = export_lst_to_excel(portshow_lst, report_data_lst, 'portcmd', 'portcmd')
        
    sfpshow_lst, portcfgshow_lst = portinfo_extract(switch_params_lst, report_data_lst)
    sfpshow_df = export_lst_to_excel(sfpshow_lst, report_data_lst, 'sfpshow', 'portinfo')
    portcfgshow_df = export_lst_to_excel(portcfgshow_lst, report_data_lst, 'portcfgshow', 'portinfo', columns_title_import = 'portcfg_columns')
    
    return portshow_df, sfpshow_df, portcfgshow_df
    
    
def connected_devices(switch_params_lst):
    fdmi_lst, nsshow_lst, nscamshow_lst = connected_devices_extract(switch_params_lst, report_data_lst)
    fdmi_df = export_lst_to_excel(fdmi_lst, report_data_lst, 'fdmi', 'connected_dev')
    nsshow_df = export_lst_to_excel(nsshow_lst, report_data_lst, 'nsshow', 'connected_dev', columns_title_import = 'nsshow_columns')
    nscamshow_df = export_lst_to_excel(nscamshow_lst, report_data_lst, 'nscamshow', 'connected_dev', columns_title_import = 'nsshow_columns')
    
    return fdmi_df, nsshow_df, nscamshow_df
    
    
def interswitch_connection(switch_params_lst):
    isl_lst, trunk_lst, porttrunkarea_lst = interswitch_connection_extract(switch_params_lst, report_data_lst)
    isl_df = export_lst_to_excel(isl_lst, report_data_lst, 'isl', 'isl')
    trunk_df = export_lst_to_excel(trunk_lst, report_data_lst, 'trunk', 'isl', columns_title_import = 'trunk_columns')
    porttrunkarea_df = export_lst_to_excel(porttrunkarea_lst, report_data_lst, 'porttrunkarea', 'isl', columns_title_import = 'porttrunkarea_columns')
    
    return isl_df, trunk_df, porttrunkarea_df
    
    
def fcrouting(switch_params_lst):    
    fcrfabric_lst, fcrproxydev_lst, fcrphydev_lst, lsan_lst, fcredge_lst, fcrresource_lst = fcr_extract(switch_params_lst, report_data_lst)
    fcrfabric_df = export_lst_to_excel(fcrfabric_lst, report_data_lst, 'fcrfabric', 'fcr', columns_title_import = 'fcrfabric_columns')
    fcrproxydev_df = export_lst_to_excel(fcrproxydev_lst, report_data_lst, 'fcrproxydev', 'fcr', columns_title_import = 'fcrproxydev_columns')
    fcrphydev_df =export_lst_to_excel(fcrphydev_lst, report_data_lst, 'fcrphydev', 'fcr', columns_title_import = 'fcrphydev_columns')
    lsan_df = export_lst_to_excel(lsan_lst, report_data_lst, 'lsan', 'fcr', columns_title_import = 'lsan_columns') 
    fcredge_df = export_lst_to_excel(fcredge_lst, report_data_lst, 'fcredge', 'fcr', columns_title_import = 'fcredge_columns')       
    fcrresource_df = export_lst_to_excel(fcrresource_lst, report_data_lst, 'fcrresource', 'fcr', columns_title_import = 'fcrresource_columns')
    
    return fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df
    
def zoning(switch_params_lst):
    cfg_lst, zone_lst, alias_lst, cfg_effective_lst, zone_effective_lst = zoning_extract(switch_params_lst, report_data_lst)
    cfg_df = export_lst_to_excel(cfg_lst, report_data_lst, 'cfg', 'zoning')
    zone_df = export_lst_to_excel(zone_lst, report_data_lst, 'zone', 'zoning', columns_title_import = 'zone_columns')
    alias_df = export_lst_to_excel(alias_lst, report_data_lst, 'alias', 'zoning', columns_title_import = 'alias_columns')
    cfg_effective_df = export_lst_to_excel(cfg_effective_lst, report_data_lst, 'cfg_effective', 'zoning', columns_title_import = 'cfg_effective_columns')
    zone_effective_df = export_lst_to_excel(zone_effective_lst, report_data_lst, 'zone_effective', 'zoning', columns_title_import = 'zone_effective_columns')
    
    return cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df       
          
    
    # return chassis_params_fabric_lst

if __name__ == "__main__":
    main()
    # config_data, report_data_lst  = config_data_check()
    # switch_params_check(config_data, report_data_lst)
    
    
    
    
    print('\nExecution successfully finished\n')