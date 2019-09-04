import pandas as pd
from files_operations import export_lst_to_excel, columns_import, dct_from_columns
from san_toolbox_parser import create_parsed_dirs, create_files_list_to_parse, santoolbox_process
from san_chassis_params import chassis_params_extract
from san_switch_params import switch_params_configshow_extract
from san_amsmaps_log import maps_params_extract
from san_fabrics import fabricshow_extract
from portcmdshow import portcmdshow_extract
from san_portinfo import portinfo_extract

"""
Main module to run
Define folders where configuration data is stored
Define SAN Assessment project folder to store all data
Define customer name
File report_info_xlsx
"""

# global list with constant vars for report operations 
# (customer name, folder to save report, biggest file name to print info on screen)
report_data_lst = []

# list with extracted customer name, supportsave folder and project folder
report_info_lst = columns_import('report', 80, 'report_data', init_file = 'report_info.xlsx') 
customer_name = rf'{report_info_lst[0]}'
project_folder = rf'{report_info_lst[1]}'
ssave_folder = rf'{report_info_lst[2]}'
# dictionary with report steps as keys. each keys has two values
# first value shows if it is required to export extracted data to excel table
# second value shows if it is required to initiate force data extraction if data have been already extracted
report_steps_dct = dct_from_columns('service_tables', 80, 'keys', 'export_to_excel', 'force_extract', init_file = 'report_info.xlsx')


print('\n\n')
print(f'ASSESSMENT FOR SAN {customer_name}'.center(125, '.'))

def config_data_check():
    
    global report_data_lst
    
    # create directories in SAN Assessment project folder 
    dir_parsed_sshow, dir_parsed_others, dir_report, dir_data_objects = create_parsed_dirs(customer_name, project_folder)

    # check for switches unparsed configuration data
    # returns list with config data files and maximum config filename length
    unparsed_lst, max_title = create_files_list_to_parse(ssave_folder)
    
    # creates list with report data
    report_data_lst = [customer_name, dir_report, dir_data_objects, max_title, report_steps_dct]
    
    # export unparsed config filenames to DataFrame and saves it to excel file
    # if report_steps_dct['unparsed_files'][0]:
    export_lst_to_excel(unparsed_lst, report_data_lst, 'unparsed_files', columns = ['sshow', 'amsmaps'])

    # returns list with parsed data
    parsed_lst, parsed_filenames_lst = santoolbox_process(unparsed_lst, dir_parsed_sshow, dir_parsed_others, max_title)
    # export parsed config filenames to DataFrame and saves it to excel file   
    # if report_steps_dct['parsed_files'][0]:
    parsed_lst_columns = ['chassis_name', 'sshow_config', 'ams_maps_log_config']
    export_lst_to_excel(parsed_filenames_lst, report_data_lst, 'parsed_files', columns = parsed_lst_columns)
    

    return parsed_lst, report_data_lst

def switch_params_check(parsed_lst, report_data_lst):

    # check configuration files to extract chassis parameters and save it to the list of lists
    chassis_params_fabric_lst = chassis_params_extract(parsed_lst, report_data_lst)
    # export chassis parameters list to DataFrame and saves it to excel file
    export_lst_to_excel(chassis_params_fabric_lst, report_data_lst, 'chassis_parameters', 'chassis')
      
    switch_params_lst, switchshow_ports_lst = switch_params_configshow_extract(chassis_params_fabric_lst, report_data_lst)
    # if report_steps_dct['switch_parameters'][0]:
    export_lst_to_excel(switch_params_lst, report_data_lst, 'switch_parameters', 'switch')
    # if report_steps_dct['switchshow_ports'][0]:
    export_lst_to_excel(switchshow_ports_lst, report_data_lst, 'switchshow_ports', 'switch', columns_title_import = 'switchshow_portinfo_columns')
        
    maps_params_fabric_lst = maps_params_extract(parsed_lst, report_data_lst)
    # export maps parameters list to DataFrame and saves it to excel file
    # if report_steps_dct['maps_parameters'][0]:
    export_lst_to_excel(maps_params_fabric_lst, report_data_lst, 'maps_parameters', 'maps')
        

    fabricshow_lst, fcrfabricshow_lst = fabricshow_extract(switch_params_lst, report_data_lst)
    # export fabrichow and fcrfabricshow lists to DataFrame and saves it to excel file
    # if report_steps_dct['fabricshow'][0]:
    export_lst_to_excel(fabricshow_lst, report_data_lst, 'fabricshow', 'fabricshow')
    # if report_steps_dct['fcrfabricshow'][0]:
    export_lst_to_excel(fcrfabricshow_lst, report_data_lst, 'fcrfabricshow', 'fabricshow', columns_title_import = 'fcr_columns')
        
    portshow_lst = portcmdshow_extract(chassis_params_fabric_lst, report_data_lst)
    # if report_steps_dct['portshow'][0][0]:
    export_lst_to_excel(portshow_lst, report_data_lst, 'portcmd', 'portcmd')
        
    sfpshow_lst, portcfgshow_lst = portinfo_extract(switch_params_lst, report_data_lst)
    # if report_steps_dct['sfpshow'][0]:
    export_lst_to_excel(sfpshow_lst, report_data_lst, 'sfpshow', 'portinfo')
    # if report_steps_dct['portcfgshow'][0]:
    export_lst_to_excel(portcfgshow_lst, report_data_lst, 'portcfgshow', 'portinfo', columns_title_import = 'portcfg_columns')
           
    
    # return chassis_params_fabric_lst

if __name__ == "__main__":
    config_data, report_data_lst  = config_data_check()
    switch_params_check(config_data, report_data_lst)
    
    
    
    
    print('\nExecution successfully finished\n')