# -*- coding: utf-8 -*-
"""
Created on Mon Jan  2 15:22:11 2023

@author: kavlasenko
"""


import os
import re
import gzip
import pandas as pd
import numpy as np

import tarfile


script_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts'

# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop


RELEASE = '0.1'

SSHOW_SECTIONS = ['SSHOW_PLOG', 'SSHOW_OS', 'SSHOW_EX', 'SSHOW_FABRIC', 
                  'SSHOW_CONDB', 'SSHOW_SERVICE', 'SSHOW_SEC', 'SSHOW_NET', 
                  'SSHOW_SYS', 'SSHOW_FICON', 'SSHOW_ISWITCH', 'SSHOW_ISCSI', 'SSHOW_ASICDB', 
                  'SSHOW_AG', 'SSHOW_FCIP', 'SSHOW_APM', 'SSHOW_AMP', 'SSHOW_CRYP', 'SSHOW_PORT', 
                  'SSHOW_DCEHSL', 'SSHOW_FLOW']


HIGH_PRIORITY_SECTIONS = ['SSHOW_EX', 'SSHOW_FABRIC', 'SSHOW_SERVICE', 
                          'SSHOW_SEC', 'SSHOW_SYS', 'SSHOW_AG', 'SSHOW_PORT']

'Section: SSHOW_ISCSI'
'Section: SSHOW_CRYP'
'Section: SSHOW_APM'
# sshow_sections_weight = {section_name:i for i, section_name in enumerate(SSHOW_SECTIONS)}

ssave_sections_stat_columns = ['directory_path', 'directory_name', 'section_name', 'hight_priority', 'ssave_filename', 'count', 'size_KB',  'ssave_basename', 'sshow_filename']



# with gzip.open('/home/joe/file.txt.gz', 'rb') as f:
#     file_content = f.read()




pattern_dct = {'sshow_sys_section': r'((.+S\d+(?:cp)?)-\d+)\.SSHOW_SYS.(?:txt.)?gz$', 'single_filename': '^(.+?.\.(\w+))\.(?:tar|txt).gz', 'amps_maps_section': r'^(.+?)\.AMS_MAPS_LOG\.(?:tar|txt).gz'}



def get_sshow_filepath(sshow_sys_section_file, sshow_dir):
    """Function takes sshow_sys configuration file and
    directory to export unpacked and concatenated sshow files as input parameters.
    Returns output filepath which is used later to export concatenated sshow files"""
    
    # sshow_name_pattern = r'((.+S\d+(?:cp)?)-\d+)\.SSHOW_SYS.(?:txt.)?gz$'
    sshow_name_pattern = pattern_dct['sshow_sys_section']
    # get filename from absolute filepath
    sshow_sys_section_filename = os.path.basename(sshow_sys_section_file)
    # drop characters after sshow collection datetime
    sshow_filename = re.search(sshow_name_pattern, sshow_sys_section_filename).group(1)
    # create filename used to export sshow sections
    sshow_filename = sshow_filename + '-SupportShow.txt'
    # combine output directory and sshow filename
    sshow_filepath = os.path.normpath(os.path.join(sshow_dir, sshow_filename))
    return sshow_filepath
    

def get_sshow_basename(sshow_sys_section_file):
    """Function returns SSHOW_SYS file basename (hostname, ip address, S#cp) 
    to filter sshow files related to sshow_sys_section_file (same S#cp)"""
    
    # sshow_name_pattern = r'((.+S\d+(?:cp)?)-\d+)\.SSHOW_SYS.(?:txt.)?gz$'
    sshow_name_pattern = pattern_dct['sshow_sys_section']
    # get filename from absolute filepath
    sshow_sys_section_filename = os.path.basename(sshow_sys_section_file)
    # extract hostname, ip address, S#cp
    sshow_section_basename = re.search(sshow_name_pattern, sshow_sys_section_filename).group(2)
    return sshow_section_basename
    
                

def add_log_entry(file_name, *args):
    """Function add lines (args) to the file_name"""
    
    # Open the file in append & read mode ('a+')
    with open(file_name, "a+") as file_object:
        appendEOL = False
        # Move read cursor to the start of file.
        file_object.seek(0)
        # Check if file is not empty
        data = file_object.read(100)
        if len(data) > 0:
            appendEOL = True
        # Iterate over each string in the list
        for log_entry in args:
            # If file is not empty then append '\n' before first line for
            # other lines always append '\n' before appending line
            if appendEOL == True:
                file_object.write("\n")
            else:
                appendEOL = True
            # Append element at the end of file
            file_object.write(log_entry)




def count_ssave_section_files_stats(sshow_sys_section_file, sshow_filepath):
    """Function counts statistics for the sections from  SSHOW_SECTIONS list 
    with the same basename as sshow_sys_section_file in directory where sshow_sys_section_file is located"""
    
    # section statistics dataframe to filter failed sections
    sw_ssave_sections_stat_df = pd.DataFrame(index=SSHOW_SECTIONS, columns=['Quantity', 'Size_KB', 'High_priority'])
    # create pattern to filter required sshow files
    sshow_sys_section_basename = get_sshow_basename(sshow_sys_section_file)
    ssave_sections_dir = os.path.dirname(sshow_sys_section_file)
    pattern = fr"{sshow_sys_section_basename}-\d+\.({'|'.join(SSHOW_SECTIONS)})\.(?:txt.)?gz$"
    
    # print(pattern)
    # print(sshow_section_basename, ssave_sections_dir)
    
    # section statistics dataframe used to concatenate sshow files
    ssave_sections_stat_current_df = pd.DataFrame(columns=ssave_sections_stat_columns)

    for file in os.listdir(ssave_sections_dir):
        # skip if file is a directory
        if not os.path.isfile(os.path.join(ssave_sections_dir, file)):
            continue
        # if file is related to sshow_sys_section_file basename
        if re.search(pattern, file):
            section_name = re.search(pattern, file).group(1)
            # count files with the same basename and section_name
            if pd.isna(sw_ssave_sections_stat_df.loc[section_name, 'Quantity']):
                sw_ssave_sections_stat_df.loc[section_name, 'Quantity'] = 1
            else:
                sw_ssave_sections_stat_df.loc[section_name, 'Quantity'] += 1
            # print(ssave_sections_stat_df.loc[section_name, 'Quantity'])
            # sshow section file size in KB
            sw_ssave_sections_stat_df.loc[section_name, 'Size_KB'] = round(os.path.getsize(os.path.join(ssave_sections_dir, file)) / 1024, 2)
            # section_names used to san audit have priority 1
            priority = 1 if section_name in HIGH_PRIORITY_SECTIONS else np.nan
            sw_ssave_sections_stat_df.loc[section_name, 'High_priority'] = priority
            # print(ssave_sections_stat_df.loc[section_name, 'Size_KB'])
            # fill values for the ssave section file in dataframe used to concatenate files later
            ssave_sections_stat_current_df.loc[len(ssave_sections_stat_current_df)] = [ssave_sections_dir, os.path.basename(ssave_sections_dir),
                                                                                   section_name, priority, file, 
                                                                                   sw_ssave_sections_stat_df.loc[section_name, 'Quantity'],
                                                                                   sw_ssave_sections_stat_df.loc[section_name, 'Size_KB'],
                                                                                   sshow_sys_section_basename,
                                                                                   os.path.basename(sshow_filepath)]
    # add empty rows for the sections for which files are absent in the directory
    absent_sections = [section for section in sw_ssave_sections_stat_df.index if not section in ssave_sections_stat_current_df['section_name'].values]
    # print(absent_sections)
    for section in absent_sections:
        ssave_sections_stat_current_df.loc[len(ssave_sections_stat_current_df), ['directory_path', 'directory_name', 'section_name']] = \
            (ssave_sections_dir, os.path.basename(ssave_sections_dir), section) 
    return sw_ssave_sections_stat_df, ssave_sections_stat_current_df


def filter_fault_sections(sw_ssave_sections_stat_df):
    """Function filters failed sections (sections with high piority with absent files or files with zero size,
    section with any piority and multiple files)"""
    
    mask_quantity_failure = sw_ssave_sections_stat_df['Quantity'] != 1
    mask_high_priority = sw_ssave_sections_stat_df['High_priority'] == 1
    mask_size_zero = sw_ssave_sections_stat_df['Size_KB'] == 0
    mask_multiple_files = sw_ssave_sections_stat_df['Quantity'] > 1
    mask_failure = (mask_high_priority & mask_quantity_failure) | (mask_high_priority & mask_size_zero) | mask_multiple_files
    sw_fault_sections_df = sw_ssave_sections_stat_df.loc[mask_failure]
    return sw_fault_sections_df 


def update_ssave_sections_stats(ssave_sections_stat_df, ssave_sections_stat_current_df):
    """Function updates dataframe with statistics for all directories.    
    It drops rows with old statistics for the directory for which 
    new sssave_sections_stat_current_df statistic is added """
    
    # drop rows for the directory for which statistics is updated
    mask_dropped_dirs = ssave_sections_stat_df['directory_path'].isin(ssave_sections_stat_current_df['directory_path'].unique())
    ssave_sections_stat_df.drop(ssave_sections_stat_df.index[mask_dropped_dirs], inplace = True)
    # add new statistics
    ssave_sections_stat_df = pd.concat([ssave_sections_stat_df, ssave_sections_stat_current_df])
    return ssave_sections_stat_df



def export_gzip_file(gzip_filepath, dest_filepath):
    """Function read gzip txt file and write its content to dest_filepath"""
    
    with gzip.open(gzip_filepath, "rt", encoding='utf-8', errors='ignore') as gzf:
        with open(dest_filepath, "a+") as dest_file:
            dest_file.write(gzf.read())
            

def export_tar_file(tar_filepath, dest_filepath):
    """Function opens tar archive, read txt file and write its conntent to dest_filepath.
    If tar archive has no files or has multiple files then warning message is displayes"""
    
    with tarfile.open(tar_filepath, "r:gz") as tf:
        # get list of all files in archive ignoring directories
        tarfile_lst = [tarinfo.name for tarinfo in tf if tarinfo.isreg()]
        if not tarfile_lst:
            print('WARNING. No configuration in tar archive.')
        elif len(tarfile_lst) > 1:
            print('WARNING. Multiple files in tar archive.')
        # read content of the first file
        tar_file_content = tf.extractfile(tarfile_lst[0]).read().decode("utf-8", errors='ignore')
        # write content to the dest_filepath
        with open(dest_filepath, "a+") as dest_file:
            dest_file.write(tar_file_content)





def get_single_section_output_filepath(input_filepath, output_dir):
    """Function takes configuration file and directory to export unpacked file as input parameters.
    Returns output filepath which is used later to export file content"""
    
    # input_filename_pattern = '^(.+?.\.(\w+))\.(?:tar|txt).gz'
    input_filename_pattern = pattern_dct['single_filename']
    output_filename = re.search(input_filename_pattern, os.path.basename(input_filepath)).group(1) + '.txt'
    output_filepath = os.path.normpath(os.path.join(output_dir, output_filename))
    return output_filepath

def export_single_section_file(input_filepath, output_filepath):
    """Function exports input_filepath (tar or gz with single txt file)
    to output_filepath"""
    
    # tar file export
    if tarfile.is_tarfile(input_filepath):
        export_tar_file(tar_filepath=input_filepath, dest_filepath=output_filepath)
    # gz file export
    else:
        export_gzip_file(gzip_filepath=input_filepath, dest_filepath=output_filepath)



def create_sshow_file(ssave_sections_stat_current_df, sshow_filepath):
    """Function concatenates sshow sections related to ssave_sections_stat_current_df file
    and writes it to the sshow_filepath"""
    
    # insert file header
    insert_sshow_header(ssave_sections_stat_current_df, sshow_filepath)
    ssave_sections_stat_current_df.set_index(keys='section_name', drop=True, inplace=True)
    for section_name in SSHOW_SECTIONS:
        if pd.notna(ssave_sections_stat_current_df.loc[section_name, 'ssave_filename']):
            ssave_section_file = os.path.normpath(os.path.join(
                ssave_sections_stat_current_df.loc[section_name, 'directory_path'],
                ssave_sections_stat_current_df.loc[section_name, 'ssave_filename']
                ))
            # insert section name
            insert_section_header(sshow_filepath, section_name)
            # write section content to sshow_filepath
            export_gzip_file(gzip_filepath=ssave_section_file, dest_filepath=sshow_filepath)
            

    
def insert_section_header(sshow_filepath, section_name):
    """Function inserts sshow section title to concatenated sshow file"""
    
    section_header_str = "| Section: " + section_name + " |"
    border_str = "+" + "-" * (len(section_header_str) - 2) + "+"
    add_log_entry(sshow_filepath, border_str, section_header_str, border_str, '\n')


def insert_sshow_header(ssave_sections_stat_current_df, sshow_filepath):
    """Function inserts header to the sshow_filepath"""
    
    title = "SupportShow rebuilt by SAN Audit Automation"
    author = 'Author: KVl'
    email = 'Email: konstantin.alx.vlasenko@gmail.com'
    date_time = f"Created: {dfop.current_datetime()}"
    release = f"with Release: {RELEASE}"
    processed_files = f"Processed {ssave_sections_stat_current_df['ssave_filename'].count()} 'SSHOW_xxx' files" 
            
    # find maximum string length
    str_lst = [title, author, email, date_time, release, processed_files]
    max_str_len = max([len(str_) for str_ in str_lst])
    # add side borders to the strings
    str_lst = ["| " + str_ + " " * (max_str_len - len(str_) + 1) + "|" for str_ in str_lst]
    # horizontal borders
    border_str = "+" + "-" * (max_str_len + 2) + "+" 
    # insert header to the sshow_filepath
    add_log_entry(sshow_filepath, border_str, *str_lst[:3], border_str, *str_lst[3:], border_str, '')
    
    

    

# ssave_sections_stat_columns = ['directory_path', 'directory_name', 'section_name', 'ssave_filename', 'count', 'size_KB',  'ssave_basename', 'sshow_filename']


# df.drop(df.index[df['line_race'] == 0], inplace = True)


# ssave_section_file = r'D:\Documents\06.CONFIGS\MTS\JAN22\mts_msc\ssave_2\Brocade_DCX-10.74.135.9-10000005339A70FF\dr03as07-c1-21-10.74.135.9-S6cp-202201311339.SSHOW_SYS.txt.gz'
sshow_sys_section_file = r'D:\Documents\06.CONFIGS\MTS\JAN22\mts_msc\ssave_2\Brocade_DCX-10.74.135.9-10000005339A70FF\dr03as07-c1-21-10.74.135.9-S6cp-202201311339.SSHOW_SYS.txt.gz'
sshow_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON'
sshow_filepath = get_sshow_filepath(sshow_sys_section_file, sshow_dir)
# sshow_file = 'supportshow_.txt'
# add_section_header(sshow_file, section_name='SSHOW_SYS')
# write_ssave_section(ssave_section_file, sshow_file, section_name='SSHOW_SYS')



sw_ssave_sections_stat_df, ssave_sections_stat_current_df = count_ssave_section_files_stats(sshow_sys_section_file, sshow_filepath)
# sw_ssave_sections_stat_df.loc['SSHOW_PLOG', 'Size_KB'] = 0
sw_fault_sections_df = filter_fault_sections(sw_ssave_sections_stat_df)
ssave_sections_stat_df = pd.DataFrame(columns=ssave_sections_stat_columns)
ssave_sections_stat_df = update_ssave_sections_stats(ssave_sections_stat_df, ssave_sections_stat_current_df)
create_sshow_file(ssave_sections_stat_current_df, sshow_filepath)


    
    
# ssave_sections_stat_df = pd.DataFrame(columns=['Directory_path', 'Directory_name', 'Section_name', 'Ssave_filename', 'Sshow_filename'])



# current_folder = r'D:\Documents\06.CONFIGS\MegafonMSK\APR21\ssave\supportsave-20-05-2021'
# for file in os.listdir(current_folder):
#     if os.path.isfile(os.path.join(current_folder, file)):
#         print('file')
#     elif os.path.isdir(os.path.join(current_folder, file)):
#         print('dir')
#     print(os.path.isfile(file))
#     print(type(file))


# os.path.dirname(ssave_section_file)


# ssave_file = r'D:\Documents\06.CONFIGS\Rencredit\Supportinfo-Thu-10-15-2020-15-00-18\SilkWorm4024-10.12.225.16-100000051E596672\fce5-V3135-1R-S0-202010151602.SSHOW_SYS.gz'
sshow_sys_section_file = r'D:\Documents\06.CONFIGS\Rencredit\Supportsave\SilkWorm4024-10.12.225.16-100000051E596672\fce5-V3135-1R-S0-202010021838.SSHOW_SYS.gz'
sshow_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON'
sshow_file = get_sshow_filepath(sshow_sys_section_file, sshow_dir)
sw_ssave_sections_stat_df, ssave_sections_stat_current_df = count_ssave_section_files_stats(sshow_sys_section_file, sshow_file)
# sw_ssave_sections_stat_df.loc['SSHOW_PLOG', 'Size_KB'] = 0
sw_fault_sections_df = filter_fault_sections(sw_ssave_sections_stat_df)
ssave_sections_stat_df = pd.DataFrame(columns=ssave_sections_stat_columns)
ssave_sections_stat_df = update_ssave_sections_stats(ssave_sections_stat_df, ssave_sections_stat_current_df)
create_sshow_file(ssave_sections_stat_current_df, sshow_file)
    


sshow_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON'
ams_maps_file = r'D:\Documents\06.CONFIGS\MTS\JAN22\mts_msc\ssave_2\Brocade_DCX-10.74.135.9-10000005339A70FF\dr03as07-c1-21_FID128-10.74.135.9-S6cp-202201311347.AMS_MAPS_LOG.txt.gz'
parsed_ams_maps_file = get_single_filepath(ams_maps_file, sshow_dir)
export_single_file(ams_maps_file, parsed_ams_maps_file)
    
sshow_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON'
ams_maps_file = r'D:\Documents\06.CONFIGS\UnixEducation\ssave\7720\DS_7720B-192.168.5.102-S0cp-202211181116.AMS_MAPS_LOG.tar.gz'
export_single_file(ams_maps_file, sshow_dir)








    