import pandas as pd 
from files_operations import columns_import

"""Module to extract switch parameters"""


def chassis_params_extract(all_config_data, max_title):
    
    
    # information string length in terminal
    str_length = max_title + 45
    # extract sshow files from configuration data list
    sshow_files = [(switch_config_data[0], switch_config_data[1]) for switch_config_data in all_config_data]
    
    print('\n\nEXTRACTING CHASSIS PARAMETERS FROM SUPPORTSHOW CONFIGURATION FILES ...')
    # extract chassis parameters from init file
    chassis_params = columns_import('chassis_params', max_title)
    
    switch_num = len(sshow_files)
    switch_counter = 1
    
    
    for sshow_file in sshow_files:
        switch_name = sshow_file[0]
        info = f'[{switch_counter} of {switch_num}]: {switch_name} chassis parameters check'
        print(info, end =" ")
        print('OK'.rjust(str_length-len(info), '.'))
        # with open(sshow_file, encoding='utf-8', errors='ignore') as file:
        #     print(f'[{switch_counter} of {switch_num}]: Checking portcfg values in {switch_name} config file\n')
        
        switch_counter += 1