"""Module to find sshow files in ssave_path folder"""

import os
import re
import shutil
import sys
from collections import defaultdict

import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
from san_automation_constants import LEFT_INDENT


def search_ssave_files(ssave_path, pattern_dct, max_title):
    """Function to create two lists with sshow_sys (single) 
    and amps_maps configs data files (single, multiple or none).
    Directors have two ".SSHOW_SYS.txt.gz" files. For Active and Standby CPs.
    Configuration file for Active CP has bigger size"""
    
    print(f'\n\nPREREQUISITES 3. SEARCHING SUPPORTSAVE CONFIGURATION FILES\n')
    print(f'Configuration data folder {ssave_path}')

    # check if ssave_path folder exist
    fsop.check_valid_path(ssave_path)
    # rellocate files for each switch in separate folder
    distribute_ssave_files(ssave_path, pattern_dct, max_title)
    verify_sshow_sys_duplication(ssave_path, pattern_dct, max_title)
    # list to save unparsed configuration data files
    discovered_sw_cfg_files_lst = []
    
    # var to count total number of ams_maps_log files
    ams_maps_num = 0
    
    # list to save length of config data file names to find max
    # required in order to proper alighnment information in terminal
    filename_size = []

    # ssave_section_filename_pattern = r'(([\w-]+?)(?:_(FID\d+))?(-(?:[0-9]{1,3}\.){3}[0-9]{1,3})?)-(S\d(?:cp)?)(?:-DP\d+)?-\d+.[\w.]+$'
    ssave_section_filename_pattern = pattern_dct['ssave_section_filename']

    # going through all directories inside ssave folder to find configurutaion data
    for root, _, files in os.walk(ssave_path):
        # var to compare supportshow files sizes (previous and current)
        sshow_prev_size = 0
        # temporary list to save ams_maps_log files in current folder
        ams_maps_current_folder_lst = []
        # assumption there is no supportshow files in current dir
        sshow_file_path = None
        amps_maps_fid_lst = []
        
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
                    scp_active = re.search(ssave_section_filename_pattern, file).group(5)

        for file in files:
            if file.endswith("AMS_MAPS_LOG.txt.gz") or file.endswith("AMS_MAPS_LOG.tar.gz"):
                scp_current_file = re.search(ssave_section_filename_pattern, file).group(5)
                fid_current_file = re.search(ssave_section_filename_pattern, file).group(3)
                if scp_current_file == scp_active:
                    # if ams_maps file for same fid found (config duplication)
                    if fid_current_file in amps_maps_fid_lst:
                        info = ' '*LEFT_INDENT + f'Mutltiple AMS_MAPS_LOG for FID {str(fid_current_file)} in folder {os.path.basename(root)}'
                        print(info, end =" ")             
                        meop.status_info('fail', max_title, len(info))
                        sys.exit()
                    ams_maps_num += 1
                    amps_maps_fid_lst.append(str(fid_current_file))
                    ams_maps_file_path = os.path.normpath(os.path.join(root, file))
                    ams_maps_current_folder_lst.append(ams_maps_file_path)
                    filename_size.append(len(file))
    
        # add info to unparsed list only if supportshow file has been found in current directory
        # if supportshow found but there is no ams_maps files then empty ams_maps list appended to config set 
        if sshow_file_path:
            discovered_sw_cfg_files_lst.append([sshow_file_path, tuple(ams_maps_current_folder_lst)])
            if not ams_maps_current_folder_lst:
                info = ' '*LEFT_INDENT + f'No AMS_MAPS_LOG file found in folder {os.path.basename(root)}'
                print(info, end =" ")             
                meop.status_info('warning', max_title, len(info))
                display_continue_request()
                       
    sshow_num = len(discovered_sw_cfg_files_lst)
    print(f'SSHOW_SYS: {sshow_num}, AMS_MAPS_LOG: {ams_maps_num}, Total: {sshow_num + ams_maps_num} configuration files.')
    
    if sshow_num == 0:
        print('\nNo confgiguration data found')
        sys.exit()           
    return discovered_sw_cfg_files_lst


def display_continue_request():

    reply = meop.reply_request(f'{" "*(LEFT_INDENT - 1)} Do you want to CONTINUE? (y)es/(n)o: ')
    if reply == 'n':
        sys.exit()


def distribute_ssave_files(ssave_path, pattern_dct, max_title):
    """Function to check if switch supportsave files for each switch are in individual
    folder. If not create folder for each swicth met in current folder and move files 
    to corresponding folders."""
    
    # ssave_section_filename_pattern = r'(([\w-]+?)(?:_(FID\d+))?(-(?:[0-9]{1,3}\.){3}[0-9]{1,3})?)-(S\d(?:cp)?)(?:-DP\d+)?-\d+.[\w.]+$'
    ssave_section_filename_pattern = pattern_dct['ssave_section_filename']

    # going through all directories inside ssave folder to find configurutaion data
    for root, _, files in os.walk(ssave_path):               
        
        # find file groups. group name is the combination of switchname and ip address
        files_group_set = find_files_groups(root, files, ssave_section_filename_pattern, max_title)
        if len(files_group_set) > 1:
            create_group_folders(root, files_group_set, max_title)
            distribute_files_by_folders(root, files, ssave_section_filename_pattern, max_title)


def find_files_groups(root_directory, files, ssave_section_filename_pattern, max_title):
    """Function finds files group names in the root directory. 
    Group name is the file basename (combination of switchname and ip address)"""

    files_group_set = set()
    for file in files:
        files_group_name = extract_ssave_section_file_basename(file, ssave_section_filename_pattern)
        if files_group_name:
            files_group_set.add(files_group_name)
        else:
            info = ' '*LEFT_INDENT + f'Unknown file {file} found in folder {os.path.basename(root_directory)}'
            print(info, end =" ")             
            meop.status_info('warning', max_title, len(info))
            display_continue_request()
    return files_group_set


def create_group_folders(root_directory, files_group_set, max_title):
    """Function creates folders in the current root directory with names from  files_group_set"""

    for files_group_name in files_group_set:
        files_group_folder = os.path.join(root_directory, files_group_name)
        fsop.create_folder(files_group_folder, max_title)


def distribute_files_by_folders(root_directory, files, ssave_section_filename_pattern, max_title):
    """Function redistributes ssave files by corresponding folders with ssave file basename as folder names"""

    for file in files:
        files_group_folder = extract_ssave_section_file_basename(file, ssave_section_filename_pattern)
        if files_group_folder:
            path_to_move = os.path.join(root_directory, files_group_folder)
            # moving file to destination config folder
            info = ' '*LEFT_INDENT + f'{file} moving'
            print(info, end =" ") 
            try:
                shutil.move(os.path.join(root_directory, file), path_to_move)
            except shutil.Error:
                meop.status_info('fail', max_title, len(info))
                sys.exit()
            else:
                meop.status_info('ok', max_title, len(info))


def extract_ssave_section_file_basename(filename, ssave_section_filename_pattern):
    """Basename is combination of switchname and ip address"""

    if re.search(ssave_section_filename_pattern, filename):
        fid = re.search(ssave_section_filename_pattern, filename).group(3)
        if fid:
            switchname = re.search(ssave_section_filename_pattern, filename).group(2)
            ip_address = re.search(ssave_section_filename_pattern, filename).group(4)
            ssave_section_file_basename = switchname
            if ip_address:
                ssave_section_file_basename = ssave_section_file_basename + ip_address
        else:
            ssave_section_file_basename = re.search(ssave_section_filename_pattern, filename).group(1)
        return ssave_section_file_basename
        

def verify_sshow_sys_duplication(ssave_path, pattern_dct, max_title):
    """Function to check if there is SSHOW_SYS with the same S#cp in the folder"""

    # scp_pattern = r'.+(S\d+(?:cp)?)-\d+\.SSHOW_SYS.(?:txt.)?gz$'
    sshow_sys_section_pattern = pattern_dct['sshow_sys_section']
    sshow_duplicated = False

    for root, _, files in os.walk(ssave_path):
        scp_dct = defaultdict(int)
        for file in files:
            if re.match(sshow_sys_section_pattern, file):                
                scp_dct[re.search(sshow_sys_section_pattern, file).group(3)] += 1
        
        multiple_scp_lst = [key for key in scp_dct if scp_dct[key] > 1]

        if multiple_scp_lst:
            info = ' '*LEFT_INDENT + f'Mutltiple SHOW_SYS {", ".join(multiple_scp_lst)} instances in folder {os.path.basename(root)}'
            print(info, end =" ")             
            meop.status_info('fail', max_title, len(info))
            sshow_duplicated = True

    if sshow_duplicated:
        sys.exit()