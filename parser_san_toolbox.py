'''Module to find configuration data and parse it with SANToolbox tool'''


import os.path
import re
import shutil
import subprocess
import sys
from datetime import date
from os import walk

from common_operations_filesystem import check_valid_path, create_folder
from common_operations_miscellaneous import status_info

# SAN Toolbox.exe path
santoolbox_path = os.path.normpath(r"C:\\Program Files\\SAN Toolbox - Reloaded\\SAN Toolbox.exe")


def create_parsed_dirs(customer_title, project_path, max_title):
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
    
    print(f'\n\nPREREQUISITES 1. CREATING REQUIRED DIRECTORIES\n')
    print(f'Project folder {project_path}')
    # define folder and subfolders to save configuration data (supportsave and ams_maps files)
    santoolbox_parsed_dir = f'santoolbox_parsed_data_{customer_title}'
    santoolbox_parsed_sshow_path = os.path.join(project_path, santoolbox_parsed_dir, 'supportshow')
    santoolbox_parsed_others_path = os.path.join(project_path, santoolbox_parsed_dir, 'others')
    create_folder(santoolbox_parsed_sshow_path, max_title)
    create_folder(santoolbox_parsed_others_path, max_title)  
        
    # define folder san_assessment_report to save excel file with parsed configuration data
    san_assessment_report_dir = f'report_{customer_title}_' + current_date
    san_assessment_report_path = os.path.join(os.path.normpath(project_path), san_assessment_report_dir)   
    create_folder(san_assessment_report_path, max_title)
    
    # define folder to save obects extracted from configuration files
    data_objects_dir = f'data_objects_{customer_title}'
    data_objects_path = os.path.join(os.path.normpath(project_path), data_objects_dir)
    create_folder(data_objects_path, max_title)

    return santoolbox_parsed_sshow_path, santoolbox_parsed_others_path, san_assessment_report_path, data_objects_path



def find_max_title(ssave_path):
    """Function to find maximum cinfiguration file length for display puproses"""

    # check if ssave_path folder exist
    check_valid_path(ssave_path)
    
    # list to save length of config data file names to find max
    # required in order to proper alighnment information in terminal
    filename_size = []

    # going through all directories inside ssave folder to find configurutaion data
    for root, _, files in os.walk(ssave_path):
        for file in files:
            if file.endswith(".SSHOW_SYS.txt.gz") or file.endswith(".SSHOW_SYS.gz"):
                filename_size.append(len(file))
            elif file.endswith("AMS_MAPS_LOG.txt.gz"):
                filename_size.append(len(file))
    if not filename_size:
        print('\nNo confgiguration data found')
        sys.exit()
              
    return max(filename_size)


def create_files_list_to_parse(ssave_path, start_max_title):
    """
    Function to create two lists with unparsed supportshow and amps_maps configs data files.
    Directors have two ".SSHOW_SYS.txt.gz" files. For Active and Standby CPs
    Configuration file for Active CP has bigger size
    """
    
    print(f'\n\nPREREQUISITES 2. SEARCHING SUPPORSAVE CONFIGURATION FILES\n')
    print(f'Configuration data folder {ssave_path}')

    # check if ssave_path folder exist
    check_valid_path(ssave_path)

    separate_ssave_files(ssave_path, start_max_title)
   
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
            if file.endswith(".SSHOW_SYS.txt.gz") or file.endswith(".SSHOW_SYS.gz"):
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
              
    return unparsed_files_lst


def separate_ssave_files(ssave_path, max_title):
    """
    Function to check if switch supportsave files for each switch are in individual
    folder. If not create folder for each swicth met in current folder and move files 
    to corresponding folders.
    """
    
    # going through all directories inside ssave folder to find configurutaion data
    for root, _, files in os.walk(ssave_path):

        
        files_group_set = set()
        # sshow_regex = r'^(([\w-]+)(-(?:[0-9]{1,3}\.){3}[0-9]{1,3})?)-S\d(?:cp)?-\d+.SSHOW_SYS.(?:txt.)?gz$'
        
        filename_nofid_regex = r'(([\w-]+)(-(?:[0-9]{1,3}\.){3}[0-9]{1,3})?)-S\d(?:cp)?-\d+.[\w.]+$'
        filename_fid_regex = r'([\w-]+)_FID\d+(-(?:[0-9]{1,3}\.){3}[0-9]{1,3})?-S\d(?:cp)?-\d+.[\w.]+$'
        
        
        for file in files:
            if file.endswith(".SSHOW_SYS.txt.gz") or file.endswith(".SSHOW_SYS.gz"):                
                files_group_name = re.search(filename_nofid_regex, file).group(1)
                files_group_set.add(files_group_name)
        
        if len(files_group_set) > 1:
            for files_group_name in files_group_set:
                files_group_folder = os.path.join(root, files_group_name)
                create_folder(files_group_folder, max_title)
                
            for file in files:
                if re.match(filename_fid_regex, file):
                    switchname = re.search(filename_fid_regex, file).group(1)
                    ip_address = re.search(filename_fid_regex, file).group(2)
                    files_group_folder = switchname
                    if ip_address:
                        files_group_folder = files_group_folder + ip_address
                elif re.match(filename_nofid_regex, file):
                    files_group_folder = re.search(filename_nofid_regex, file).group(1)
                path_to_move = os.path.join(root, files_group_folder)
                
                # moving file to destination config folder
                info = ' '*16+f'{file} moving'
                print(info, end =" ") 
                try:
                    shutil.move(os.path.join(root, file),path_to_move)
                except shutil.Error:
                    status_info('fail', max_title, len(info))
                    sys.exit()
                else:
                    status_info('ok', max_title, len(info))


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
    
    print('\n\nPREREQUISITES 3. PROCESSING SUPPORTSAVE FILES WITH SANTOOLBOX\n')
    print(f'Parsed configuration files is moved to\n{os.path.dirname(path_to_move_parsed_sshow)}\n')
    
    # going throgh each configuration set (switch) in unpased list
    for i,switch_files_to_parse_lst in enumerate(all_files_to_parse_lst):
        
        # extracts switchname from supportshow filename
        switchname = re.search(r'^([\w-]+)(-\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})?-S\d(cp)?-\d+.SSHOW_SYS.(txt.)?gz$', 
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
            info = ' '*16+'No AMS_MAPS configuration found.'
            print(info, end =" ")
            status_info('skip', max_title, len(info))
            # ams_maps_files_lst_tmp.append(None)
            # ams_maps_filenames_lst_tmp.append(None)
            ams_maps_files_lst_tmp = None
            ams_maps_filenames_lst_tmp = None
        
        # append parsed configuration data filenames and filepaths to the final lists 
        parsed_files_lst.append([switchname, parsed_sshow_file, ams_maps_files_lst_tmp])
        # ams_maps_filenames_str = ', '.join(ams_maps_filenames_lst_tmp) if ams_maps_filenames_lst_tmp else None
        # parsed_filenames_lst.append([switchname, parsed_sshow_filename, ', '.join(ams_maps_filenames_lst_tmp)])
        parsed_filenames_lst.append([switchname, parsed_sshow_filename, ams_maps_filenames_lst_tmp])    
    
    print('\n')
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
    elif filename.endswith(".SSHOW_SYS.gz"):
        ending1 = '.SSHOW_SYS.gz'
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
