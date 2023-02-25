"""Module to find sshow_sys and ams_maps files in ssave_path folder to build comprehensvie sshow file"""

import os
import re
import sys

import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
from san_automation_constants import LEFT_INDENT


def search_ssave_files(ssave_path, pattern_dct, max_title):
    """Function to create two lists with sshow_sys (single) 
    and amps_maps configs data files (single, multiple or none).
    Directors have two ".SSHOW_SYS.txt.gz" files. For Active and Standby CPs.
    Configuration file for Active CP has bigger size"""
    
    print(f'\n\nPREREQUISITES 4. SEARCHING SUPPORTSAVE CONFIGURATION FILES TO EXPORT\n')
    print(f'Switch configurations folder \n{ssave_path}\n')
    
    # list to save discovered ssave configuration files
    discovered_sw_cfg_files_lst = []
    # total number of ams_maps_log files counter
    ams_maps_num = 0
    
    for root, _, files in os.walk(ssave_path):
        
        info = f"Searching sshow_sys in folder '{os.path.basename(root)}'"
        print(info, end =" ")
        
        if not files:
            meop.status_info('empty', max_title, len(info))
            continue   
        
        # find active CP sshow_sys configuration file
        sshow_sys_file_path, scp_active = search_active_sshow_sys_config(root, files, pattern_dct)
        # find active CP ams_maps configuration files
        ams_maps_current_folder_lst = search_active_ams_maps_configs(root, files, scp_active, pattern_dct, max_title)

        # add discovered files to the list
        if sshow_sys_file_path:
            meop.status_info('ok', max_title, len(info))
            discovered_sw_cfg_files_lst.append([sshow_sys_file_path, tuple(ams_maps_current_folder_lst)])
            ams_maps_num += len(ams_maps_current_folder_lst)
            if not ams_maps_current_folder_lst:
                info = ' '*LEFT_INDENT + f'No AMS_MAPS_LOG file found in folder {os.path.basename(root)}'
                print(info, end =" ")             
                meop.status_info('warning', max_title, len(info))
                meop.display_continue_request()
        else:
            meop.status_info('not found', max_title, len(info))
                       
    sshow_num = len(discovered_sw_cfg_files_lst)
    print(f'\nSSHOW_SYS: {sshow_num}, AMS_MAPS_LOG: {ams_maps_num}, Total: {sshow_num + ams_maps_num} configuration files.')
    
    if sshow_num == 0:
        print('\nNo sshow_sys files found')
        sys.exit()           
    return discovered_sw_cfg_files_lst


def search_active_sshow_sys_config(directory, files, pattern_dct):
    """Function finds active CP sshow_sys file"""

    # var to compare supportshow files sizes (previous and current)
    sshow_prev_size = 0
    sshow_sys_file_path = None
    scp_active = None

    for file in files:
        if not re.search(pattern_dct['sshow_sys_section'], file):
            continue
        # file with bigger size is Active CP configuration data file
        sshow_file_size = os.path.getsize(os.path.join(directory, file))
        if sshow_file_size > sshow_prev_size:
            sshow_sys_file_path = os.path.normpath(os.path.join(directory, file))
            # save current file size to previous file size 
            # to compare with second supportshow file size if it's found
            sshow_prev_size = sshow_file_size
            scp_active = re.search(pattern_dct['sshow_sys_section'], file).group(3)
    return sshow_sys_file_path, scp_active


def search_active_ams_maps_configs(directory, files, scp_active, pattern_dct, max_title):
    """Function finds active CP ams_maps files"""

    ams_maps_current_folder_lst = []
    amps_maps_fid_lst = []
    ssave_section_filename_pattern = pattern_dct['ssave_section_filename']

    for file in files:
        if not re.search(pattern_dct['amps_maps_section'], file):
            continue
        
        scp_current_file = re.search(ssave_section_filename_pattern, file).group(5)
        if scp_current_file != scp_active:
            continue
        
        fid_current_file = re.search(ssave_section_filename_pattern, file).group(3)
        # if ams_maps file for same fid found (config duplication)
        if fid_current_file in amps_maps_fid_lst:
            info = ' '*LEFT_INDENT + f'Mutltiple AMS_MAPS_LOG for FID {str(fid_current_file)} in folder {os.path.basename(directory)}'
            print(info, end =" ")             
            meop.status_info('fail', max_title, len(info))
            sys.exit()
        amps_maps_fid_lst.append(str(fid_current_file))
        ams_maps_file_path = os.path.normpath(os.path.join(directory, file))
        ams_maps_current_folder_lst.append(ams_maps_file_path)
    return ams_maps_current_folder_lst




