import pandas as pd
from files_operations import save_xlsx_file, columns_import
from san_toolbox_parser import create_parsed_dirs, create_files_list_to_parse, santoolbox_process
from san_chassis_params import chassis_params_extract
from san_switch_params import switch_params_configshow_extract

"""
Main module to run
Define folders where configuration data is stored
Define SAN Assessment project folder to store all data
Define customer name
"""

# This is folder where supportsave files are stored. 
ssave_folder = r'C:\Users\vlasenko\Documents\01.CUSTOMERS\Megafon\All SANs\STF\SSV_coreSTF_20190419'
# This folder with SAN Assessment project
project_folder = r'C:\Users\vlasenko\Documents\01.CUSTOMERS\Megafon\All SANs\STF'
# Customer name
customer_name = r'stf_cust'

print('\n\n')
print(f'ASSESSMENT FOR SAN {customer_name}'.center(125, '.')) 

def config_data_check():
    
    # create directories in SAN Assessment project folder 
    dir_parsed_sshow, dir_parsed_others, dir_report = create_parsed_dirs(customer_name, project_folder)

    # check for switches unparsed configuration data
    # returns list with config data files and maximum config filename length
    unparsed_lst, max_title = create_files_list_to_parse(ssave_folder)
    # export unparsed config files to DataFrame
    unparsed_files_df = pd.DataFrame(unparsed_lst, columns = ['sshow', 'amsmaps'])
    # save unparsed DataFrame to serice report excel file
    save_xlsx_file(unparsed_files_df, 'unparsed_files', customer_name, 'service', dir_report, max_title)

    # returns list with parsed data
    parsed_lst, parsed_filenames_lst = santoolbox_process(unparsed_lst, dir_parsed_sshow, dir_parsed_others, max_title)
    # # export parsed config filenames to DataFrame
    parsed_filenames_df = pd.DataFrame(parsed_filenames_lst, columns = ['switch_name', 'sshow_config', 'ams_maps_log_config'])
    # save unparsed DataFrame to serice report excel file
    save_xlsx_file(parsed_filenames_df, 'parsed_files', customer_name, 'service', dir_report, max_title)

    return parsed_lst, dir_report, max_title

def switch_params_check(parsed_lst, dir_report, max_title):
    
    
    chassis_columns = columns_import('columns', 'chassis_columns', max_title)
    chassis_params_fabric_lst = chassis_params_extract(parsed_lst, max_title)
    
    chassis_params_fabric_df = pd.DataFrame(chassis_params_fabric_lst, columns= chassis_columns)
    
    save_xlsx_file(chassis_params_fabric_df, 'chassis_params', customer_name, 'service', dir_report, max_title)
    

    
    switch_columns_configshow = columns_import('columns', 'switch_columns_configshow', max_title)

    switch_params_configshow_lst = switch_params_configshow_extract(chassis_params_fabric_lst, chassis_columns, max_title)
    
    switch_columns_configshow_df = pd.DataFrame(switch_params_configshow_lst, columns = switch_columns_configshow)
    
    save_xlsx_file(switch_columns_configshow_df, 'switch_params_configshow', customer_name, 'service', dir_report, max_title)
    
    
    
    

if __name__ == "__main__":
    config_data, dir_report, max_title = config_data_check()
    switch_params_check(config_data, dir_report, max_title)
    
    print('\nExecution successfully finished\n')