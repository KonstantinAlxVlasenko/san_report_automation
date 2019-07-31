import pandas as pd
from files_operations import save_xlsx_file
from san_toolbox_parser import create_parsed_dirs, create_files_list_to_parse, santoolbox_process

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

print(f'\nAssessment for SANs for {customer_name}\n'.center(120, '-'))

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
    parsed_filenames_df = pd.DataFrame(parsed_filenames_lst, columns = ['sshow', 'amsmaps'])
    # save unparsed DataFrame to serice report excel file
    save_xlsx_file(parsed_filenames_df, 'parsed_files', customer_name, 'service', dir_report, max_title)

    return parsed_lst

if __name__ == "__main__":
    config_data_check()
    print('\nExecution successfully finished')