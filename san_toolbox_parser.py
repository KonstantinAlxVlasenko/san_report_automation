import os.path
import re
import shutil
import subprocess
import sys
from os import walk
from files_operations import create_folder, check_valid_path, status_info
from datetime import date

'''Module to find configuration data and parse it with SANToolbox tool'''

# SAN Toolbox.exe path
santoolbox_path = "C:\\Program Files (x86)\\SAN Toolbox - Reloaded\\san toolbox.exe"



def create_parsed_dirs(customer_title, project_path):
    """
    Function to create three folders.
    Folder to save parsed with SANToolbox supportshow data files.
    Folder to save parsed with SANToolbox all others data files.
    Folder to save excel file with parsed configuration data files
    If it is not possible to create any folder script stops.
    """
    
    # check if project folders exist
    check_valid_path(project_path)
    
    # current date
    current_date = str(date.today())   
    
    print(f'\n\nCREATING REQUIRED DIRECTORIES ...\n')
    print(f'Project folder {project_path}')
    # define folder and subfolders to save configuration data (supportsave and ams_maps files)
    santoolbox_parsed_dir = f'santoolbox_parsed_data_{customer_title}'
    santoolbox_parsed_sshow_path = os.path.join(project_path, santoolbox_parsed_dir, 'supportshow')
    santoolbox_parsed_others_path = os.path.join(project_path, santoolbox_parsed_dir, 'others')
    create_folder(santoolbox_parsed_sshow_path)
    create_folder(santoolbox_parsed_others_path)  
        
    # define folder san_assessment_report to save excel file with parsed configuration data
    san_assessment_report_dir = f'report_{customer_title}_' + current_date
    san_assessment_report_path = os.path.join(os.path.normpath(project_path), san_assessment_report_dir)   
    create_folder(san_assessment_report_path)

    return santoolbox_parsed_sshow_path, santoolbox_parsed_others_path, san_assessment_report_path


def create_files_list_to_parse(ssave_path):
    """
    Function to create two lists with unparsed supportshow and amps_maps configs data files.
    Directors have two ".SSHOW_SYS.txt.gz" files. For Active and Standby CPs
    Configuration file for Active CP has bigger size
    """
    
    print(f'\n\nCHECCKING CONFIGURATION DATA ...\n')
    print(f'Configuration data folder {ssave_path}')

    # check if ssave_path folder exist
    check_valid_path(ssave_path)
   
    # list to save unparsed configuration data files
    unparsed_files_lst = []
    
    # var to count total number of ams_maps_log files
    ams_maps_num = 0
    
    # list to save length of config data file names to find max
    # required in order to proper alighnment information in terminal
    filename_size = []

    # going through all directories inside ssave folder to find configurutaion data
    for root, _, files in os.walk(ssave_path):
        # var to compare supportshow files sizes (previous and current)
        sshow_prev_size = 0
        # temporary list to save ams_maps_log files in current folder
        ams_maps_files_lst_tmp = []
        # assumption there is no supportshow files in current dir
        sshow_file_path = None
        
        for file in files:
            if file.endswith(".SSHOW_SYS.txt.gz"):
                # var to save current supportshow file size and compare it with next supportshow file size
                # file with bigger size is Active CP configuration data file
                sshow_file_size = os.path.getsize(os.path.join(root, file))
                if sshow_file_size > sshow_prev_size:
                    sshow_file_path = os.path.normpath(os.path.join(root, file))
                    # save current file size to previous file size 
                    # to compare with second supportshow file size if it's found
                    sshow_prev_size = sshow_file_size
                    filename_size.append(len(file))

            elif file.endswith("AMS_MAPS_LOG.txt.gz"):
                ams_maps_num += 1
                ams_maps_file_path = os.path.normpath(os.path.join(root, file))
                ams_maps_files_lst_tmp.append(ams_maps_file_path)
                filename_size.append(len(file))
        
        # add info to unparsed list only if supportshow file has been found in current directory
        # if supportshow found but there is no ams_maps files then empty ams_maps list appended to config set 
        if sshow_file_path:
            unparsed_files_lst.append([sshow_file_path, tuple(ams_maps_files_lst_tmp)])
            
    sshow_num = len(unparsed_files_lst)
    print(f'SSHOW_SYS: {sshow_num}, AMS_MAPS_LOG: {ams_maps_num}, Total: {sshow_num+ams_maps_num} configuration files.')
    
    if sshow_num == 0:
        print('\nNo confgiguration data found')
        sys.exit()
              
    return unparsed_files_lst, max(filename_size)


def santoolbox_process(all_files_to_parse_lst, path_to_move_parsed_sshow, path_to_move_parsed_others, max_title):    
    """
    Check through unparsed list for configuration data sets for each switch.  
    Unparsed list format  [[unparsed sshow, (unparsed ams_maps, unparsed amps_maps ...)], []]
    """
    # list to save parsed configuration data files with full path
    parsed_files_lst = []
    # list to save parsed configuration data files names
    parsed_filenames_lst = []
    # number of configuration data sets (one set is config files for one switch)
    config_set_num = len(all_files_to_parse_lst)
    
    print('\n\nPROCESSING CONFIGURATION FILES WITH SANTOOLBOX ... \n')
    print(f'Parsed configuration files is moved to\n{os.path.dirname(path_to_move_parsed_sshow)}\n')
    
    # going throgh each configuration set (switch) in unpased list
    for i,switch_files_to_parse_lst in enumerate(all_files_to_parse_lst):
        
        # extracts switchname from supportshow filename
        switchname = re.search(r'^([\w-]+)(-\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})?-S\dcp-\d+.SSHOW_SYS.txt.gz$', 
                                os.path.basename(switch_files_to_parse_lst[0])).group(1)
        # number of ams_maps_log files in current configuration set (switch)
        ams_maps_config_num = len(switch_files_to_parse_lst[1])
        print(f'[{i+1} of {config_set_num}]: {switchname}. Number of configs: {ams_maps_config_num+1} ...')
        
        # calling santoolbox_parser function which parses SHOW_SYS file with SANToolbox
        parsed_sshow_file = santoolbox_parser(switch_files_to_parse_lst[0], path_to_move_parsed_sshow, max_title)
        parsed_sshow_filename = os.path.basename(parsed_sshow_file)
        
        # tmp lists to save parsed AMS_MAPS_LOG filenames and filepaths
        ams_maps_files_lst_tmp = []
        ams_maps_filenames_lst_tmp = []
        if ams_maps_config_num > 0:
            for ams_maps_config in switch_files_to_parse_lst[1]:
                # calling santoolbox_parser function which parses AMS_MAPS_LOG file with SANToolbox
                parsed_amsmaps_file = santoolbox_parser(ams_maps_config, path_to_move_parsed_others, max_title)
                # append filenames and filepaths to tmp lists
                ams_maps_files_lst_tmp.append(parsed_amsmaps_file)
                ams_maps_filenames_lst_tmp.append(os.path.basename(parsed_amsmaps_file))
        else:
            print('No AMS_MAPS configuration found.')
            ams_maps_files_lst_tmp.append(None)
            ams_maps_filenames_lst_tmp.append(None)
        
        # append parsed configuration data filenames and filepaths to the final lists 
        parsed_files_lst.append([switchname, parsed_sshow_file, tuple(ams_maps_files_lst_tmp)])
        parsed_filenames_lst.append([switchname, parsed_sshow_filename, ', '.join(ams_maps_filenames_lst_tmp)])    
    
    return parsed_files_lst, parsed_filenames_lst


def santoolbox_parser(file, path_to_move_parsed_data, max_title):
    """
    Function to process unparsed ".SSHOW_SYS.txt.gz" and  "AMS_MAPS_LOG.txt.gz" files with SANToolbox.
    """  
    # split filepath to directory and filename
    filedir, filename = os.path.split(file)
    if filename.endswith(".SSHOW_SYS.txt.gz"):
        ending1 = '.SSHOW_SYS.txt.gz'
        ending2 = '-SupportShow.txt'
        option = 'r'
    elif filename.endswith('AMS_MAPS_LOG.txt.gz'):     
        ending1 = 'AMS_MAPS_LOG.txt.gz'
        ending2 = 'AMS_MAPS_LOG.txt.gz.txt'
        option = 'd'
    
    # information string
    info = ' '*16+f'{filename} processing'
    print(info, end =" ")
    
    # after parsing filename is changed
    filename = filename.replace(ending1, ending2)
    
    # run SANToolbox if only config file hasn't been parsed before 
    if not os.path.isfile(os.path.join(path_to_move_parsed_data, filename)):
        try:
            subprocess.call(f'"{santoolbox_path}" -{option} "{file}"', shell=True)
        except subprocess.CalledProcessError:
            status_info('fail', max_title, len(info))
        except OSError:
            status_info('fail', max_title, len(info))
            print('SANToolbox program is not found ')
            sys.exit()
        else:
            status_info('ok', max_title, len(info))
        
        # moving file to destination config folder
        info = ' '*16+f'{filename} moving'
        print(info, end =" ") 
        try:
            shutil.move(os.path.join(filedir, filename),path_to_move_parsed_data)
        except shutil.Error:
            status_info('fail', max_title, len(info))
            sys.exit()
        except FileNotFoundError:
            status_info('fail', max_title, len(info))
            print('The system cannot find the file specified.\nCHECK that SANToolbox is CLOSED before run the script.')
            sys.exit()
        else:
            status_info('ok', max_title, len(info))
    else:
        status_info('skip', max_title, len(info))
    
    return os.path.normpath(os.path.join(path_to_move_parsed_data, filename))