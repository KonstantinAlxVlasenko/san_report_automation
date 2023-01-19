import os
import re
import gzip
import pandas as pd
import tarfile

import utilities.database_operations as dbop
import utilities.module_execution as meop

pattern_dct = {'sshow_sys_section': r'((.+S\d+(?:cp)?)-\d+)\.SSHOW_SYS.(?:txt.)?gz$', 
'single_filename': '^(.+?.\.(\w+))\.(?:tar|txt).gz', 
'amps_maps_section': r'^(.+?)\.AMS_MAPS_LOG\.(?:tar|txt).gz'}

SSHOW_SECTIONS = ['SSHOW_PLOG', 'SSHOW_OS', 'SSHOW_EX', 'SSHOW_FABRIC', 
                  'SSHOW_CONDB', 'SSHOW_SERVICE', 'SSHOW_SEC', 'SSHOW_NET', 
                  'SSHOW_SYS', 'SSHOW_FICON', 'SSHOW_ISWITCH', 'SSHOW_ISCSI', 'SSHOW_ASICDB', 
                  'SSHOW_AG', 'SSHOW_FCIP', 'SSHOW_APM', 'SSHOW_AMP', 'SSHOW_CRYP', 'SSHOW_PORT', 
                  'SSHOW_DCEHSL', 'SSHOW_FLOW']

RELEASE = '0.1'

def export_gzip_file(gzip_filepath, dest_filepath):
    """Function read gzip txt file and write its content to dest_filepath"""
    
    with gzip.open(gzip_filepath, "rt", encoding='utf-8', errors='ignore') as gzf:
        with open(dest_filepath, "a+", encoding='utf-8', errors='ignore') as dest_file:
            for line in gzf:
                tab_free_line = line.replace('\t', ' ')
                dest_file.write(tab_free_line)
            # dest_file.write(gzf.read())
            

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
            for line in tar_file_content:
                tab_free_line = line.replace('\t', ' ')
                dest_file.write(tab_free_line)
            # dest_file.write(tar_file_content)


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
    insert_sshow_footer(sshow_filepath)
            

    
def insert_section_header(sshow_filepath, section_name):
    """Function inserts sshow section title to concatenated sshow file"""
    
    section_header_str = "| Section: " + section_name + " |"
    border_str = "+" + "-" * (len(section_header_str) - 2) + "+"
    dbop.add_log_entry(sshow_filepath, border_str, section_header_str, border_str, '\n')


def insert_sshow_footer(sshow_filepath):
    """Function inserts sshow section title to concatenated sshow file"""
    
    footer_str = "| ... rebuilt finished |"
    border_str = "+" + "-" * (len(footer_str) - 2) + "+"
    dbop.add_log_entry(sshow_filepath, border_str, footer_str, border_str, '\n')


def insert_sshow_header(ssave_sections_stat_current_df, sshow_filepath):
    """Function inserts header to the sshow_filepath"""
    
    title = "SupportShow rebuilt by SAN Audit Automation"
    author = 'Author: KVl'
    email = 'E-mail: konstantin.alx.vlasenko@gmail.com'
    date_time = f"Created: {meop.current_datetime()}"
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
    dbop.add_log_entry(sshow_filepath, border_str, *str_lst[:3], border_str, *str_lst[3:], border_str, '')