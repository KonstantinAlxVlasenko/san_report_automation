import os
import re
import sys

import pandas as pd
from .ssave_export_filepath import (
    get_single_section_output_filepath,
    get_single_section_output_secondary_filepath, get_sshow_filepath)

import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
from san_automation_constants import LEFT_INDENT

from .sshow_build import build_sshow_file, export_single_section_file
from .sshow_stats import (count_ssave_section_files_stats,
                          filter_fault_sections, update_ssave_sections_stats)


def export_ssave_files(san_ssave_files_lst, path_to_move_sshow, path_to_move_others, ssave_sections_stats_df, max_title):    
    """Check through list for configuration sets for each switch.  
    Config set for each switch is ssave_sys file and list of ssave_ams_maps files."""
    

    # list to save parsed configuration data files with full path
    exported_files_lst = []
    # list to save parsed configuration data files names
    exported_filenames_lst = []
    # number of configuration data sets (one set is config files for one switch)
    config_set_num = len(san_ssave_files_lst)

    # 
    export_status_lst = []

    ssave_sections_stats_columns = ['directory_path', 'directory_name', 'section_name', 
                                'hight_priority', 'ssave_filename', 'count', 'size_KB',  
                                'ssave_basename', 'sshow_filename']

    if ssave_sections_stats_df is None:
        ssave_sections_stats_df = pd.DataFrame(columns=ssave_sections_stats_columns)
    
    print('\n\nPREREQUISITES 4. EXPORTING SUPPORTSAVE FILES\n')
    print(f'Parsed configuration files are moved to\n{os.path.dirname(path_to_move_sshow)}\n')
    
    # going throgh each configuration set (switch) in unpased list
    for i, switch_ssave_files_lst in enumerate(san_ssave_files_lst):
        
        # extracts switchname from supportshow filename
        switchname = re.search(r'^([\w-]+)(-\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})?-S\d(cp)?-\d+.SSHOW_SYS.(txt.)?gz$', 
                                os.path.basename(switch_ssave_files_lst[0])).group(1)
        # number of ams_maps_log files in current configuration set (switch)
        ssave_ams_maps_files_num = len(switch_ssave_files_lst[1])
        print(f'[{i+1} of {config_set_num}]: {switchname}. Number of configs: {ssave_ams_maps_files_num+1} ...')
        
        # build sshow file from ssave sshow sections
        sshow_filepath, ssave_sections_stats_df = pull_switch_configuration_file(switch_ssave_files_lst[0], 
                                                            path_to_move_sshow, 
                                                            ssave_sections_stats_df, max_title, export_status_lst)
        sshow_filename = os.path.basename(sshow_filepath)
        
        # tmp lists to save parsed AMS_MAPS_LOG filenames and filepaths
        ams_maps_files_lst_tmp = []
        ams_maps_filenames_lst_tmp = []
        if ssave_ams_maps_files_num > 0:
            for ssave_ams_maps_file in switch_ssave_files_lst[1]:
                # calling santoolbox_parser function which parses AMS_MAPS_LOG file with SANToolbox
                amsmaps_filepath, _ = pull_switch_configuration_file(ssave_ams_maps_file, 
                                                    path_to_move_others, 
                                                    ssave_sections_stats_df, max_title, export_status_lst)

                # append filenames and filepaths to tmp lists
                ams_maps_files_lst_tmp.append(amsmaps_filepath)
                ams_maps_filenames_lst_tmp.append(os.path.basename(amsmaps_filepath))
        else:
            info = ' '*LEFT_INDENT + 'No AMS_MAPS configuration found.'
            print(info, end =" ")
            meop.status_info('skip', max_title, len(info))
            ams_maps_files_lst_tmp = None
            ams_maps_filenames_lst_tmp = None
        
        # append parsed configuration data filenames and filepaths to the final lists 
        exported_files_lst.append([switchname, sshow_filepath, ams_maps_files_lst_tmp])
        exported_filenames_lst.append([switchname, sshow_filename, ams_maps_filenames_lst_tmp])    
    print('\n')
    return exported_files_lst, exported_filenames_lst, ssave_sections_stats_df, export_status_lst


pattern_dct = {'sshow_sys_section': r'((.+S\d+(?:cp)?)-\d+)\.SSHOW_SYS.(?:txt.)?gz$', 
'single_filename': '^(.+?.\.(\w+))\.(?:tar|txt).gz', 
'amps_maps_section': r'^(.+?)\.AMS_MAPS_LOG\.(?:tar|txt).gz'}


def pull_switch_configuration_file(ssave_section_file, output_dir, ssave_sections_stats_df, max_title, export_status_lst):
    """Function to process unparsed ".SSHOW_SYS.txt.gz" and  "AMS_MAPS_LOG.txt.gz" files with SANToolbox."""

    ssave_section_filename = os.path.basename(ssave_section_file)

    # information string
    info = ' '*LEFT_INDENT+f'{ssave_section_filename} processing'
    
    if re.search(pattern_dct['sshow_sys_section'], ssave_section_filename):
        exported_switch_config_filepath = get_sshow_filepath(ssave_section_filename, output_dir)
        exported_switch_config_secondary_filepath = ''
        config_type = 'sshow'
    elif re.search(pattern_dct['amps_maps_section'], ssave_section_filename):
        exported_switch_config_filepath = get_single_section_output_filepath(ssave_section_filename, output_dir)
        exported_switch_config_secondary_filepath = get_single_section_output_secondary_filepath(ssave_section_filename, output_dir)
        config_type = 'maps'
    else:
        print(info, end =" ")
        meop.status_info('unknown', max_title, len(info))
        return '', ssave_sections_stats_df

    config_exist_lst = fsop.validate_files(exported_switch_config_filepath, exported_switch_config_secondary_filepath)
    if config_exist_lst:
        print(info, end =" ")
        export_status_lst.append(meop.status_info('skip', max_title, len(info)))
        return config_exist_lst[0], ssave_sections_stats_df

    if config_type == 'sshow':
        ssave_sections_stats_df, ssave_sections_stats_current_df =\
            validate_sshow_section_files(ssave_section_file, exported_switch_config_filepath, ssave_sections_stats_df)
        # combine and export sshow sections files to exported_switch_config_filepath
        build_sshow_file(ssave_sections_stats_current_df, exported_switch_config_filepath)
    elif config_type == 'maps':
        export_single_section_file(input_filepath=ssave_section_file, output_filepath=exported_switch_config_filepath)

    print(info, end =" ")
    if fsop.validate_files(exported_switch_config_filepath):
        export_status_lst.append(meop.status_info('ok', max_title, len(info)))
    else:
        export_status_lst.append(meop.status_info('fail', max_title, len(info)))
    return exported_switch_config_filepath, ssave_sections_stats_df


def validate_sshow_section_files(ssave_sys_section_file, exported_switch_config_filepath, ssave_sections_stats_df):
    """Function to verify sshow sections with high priority located in follder and there is no files duplication"""

    ssave_sys_section_filename = os.path.basename(ssave_sys_section_file)
    sw_ssave_sections_stats_df, ssave_sections_stats_current_df = \
        count_ssave_section_files_stats(ssave_sys_section_file, sshow_filepath=exported_switch_config_filepath)
    sw_fault_sections_df, multiple_sshow_section_files_warning_on = filter_fault_sections(sw_ssave_sections_stats_df)
    if not sw_fault_sections_df.empty:
        input_option = None
        print('\n')
        while multiple_sshow_section_files_warning_on or \
                (not multiple_sshow_section_files_warning_on and not input_option in ['c']):
            display_fault_sections(sw_fault_sections_df, ssave_sys_section_filename)
            operation_options_lst = get_operation_options(multiple_sshow_section_files_warning_on)
            display_menu_options(operation_options_lst)
            input_option = meop.reply_request("Choose option: ", reply_options=operation_options_lst, show_reply=True)
            if input_option == 'r':
                sw_ssave_sections_stats_df, ssave_sections_stats_current_df = \
                    count_ssave_section_files_stats(ssave_sys_section_file, sshow_filepath=exported_switch_config_filepath)
                sw_fault_sections_df, multiple_sshow_section_files_warning_on = filter_fault_sections(sw_ssave_sections_stats_df)
                print('Directory rescanned')
            elif input_option == 'x':
                print('Stop program execution')
                sys.exit()
            print('\n')
    ssave_sections_stats_df = update_ssave_sections_stats(ssave_sections_stats_df, ssave_sections_stats_current_df)
    return ssave_sections_stats_df, ssave_sections_stats_current_df


def get_operation_options(multiple_sshow_section_files_warning_on):
    operation_options_lst = ['r', 'c', 'x']
    if multiple_sshow_section_files_warning_on:
        operation_options_lst.remove('c')
    return operation_options_lst


def display_fault_sections(sw_fault_sections_df, ssave_section_filename):

    separator_len = get_separator_len(sw_fault_sections_df)
    print('-' * len(ssave_section_filename))
    print(ssave_section_filename)
    print('-' * separator_len)
    print(sw_fault_sections_df)
    print('-' * separator_len)


def get_separator_len(sw_fault_sections_df):
    """Function returns length of the printed sw_fault_sections_df DataFrame upper border"""

    # find length all column names
    column_names_length = len(' '.join(sw_fault_sections_df.columns)) + 1
    # find max index str length
    max_section_name_length = sw_fault_sections_df.index.str.len().max()
    index_name_length = len(sw_fault_sections_df.index.name)
    section_name_length = max_section_name_length if max_section_name_length > index_name_length else index_name_length
    # total separator length
    separator_len = section_name_length + column_names_length
    return separator_len


def display_menu_options(operation_options_lst):
    """Function displays operation options"""

    print("\nR/r - Rescan files in folder")
    if 'c' in operation_options_lst:
        print("C/c - Ignore and continue")
    print("X/x - Stop program and exit\n")