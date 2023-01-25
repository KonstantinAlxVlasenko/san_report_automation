"""Module to distribute ssave files from the same switch by folders"""

import os
import re
import shutil
import sys

import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
from san_automation_constants import LEFT_INDENT


def distribute_ssave_files(ssave_path, pattern_dct, max_title):
    """Function to check if switch supportsave files for each switch are in individual
    folder. If not create folder for each swicth met in current folder and move files 
    to corresponding folders."""
    
    ssave_section_filename_pattern = pattern_dct['ssave_section_filename']

    # going through all directories inside ssave folder to find configurutaion data
    for root, _, files in os.walk(ssave_path):               
        
        # print(files)

        info = f"Distributing files in folder '{os.path.basename(root)}'"
        
        if not files:
            print(info, end =" ")
            meop.status_info('empty', max_title, len(info))
            continue                  
        
        # find file groups. group name is the combination of switchname and ip address
        files_group_set = find_files_groups(root, files, ssave_section_filename_pattern, max_title)
        if len(files_group_set) > 1:
            # print('\n')
            print(info)
            create_group_folders(root, files_group_set, max_title)
            distribute_files_by_folders(root, files, ssave_section_filename_pattern, max_title)
        else:
            print(info, end =" ")
            meop.status_info('skip', max_title, len(info))


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
            meop.display_continue_request()
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