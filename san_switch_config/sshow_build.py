"""Module to build supportshow and ams_maps files"""

import gzip
import os
import tarfile

import pandas as pd

import utilities.database_operations as dbop
import utilities.module_execution as meop
import utilities.report_operations as report
from san_automation_constants import RELEASE

from .sshow_stats import SSHOW_SECTIONS


def export_gzip_file(gzip_filepath, dest_filepath):
    """Function read gzip txt file and write its content to dest_filepath"""
    
    with gzip.open(gzip_filepath, "rt", encoding='utf-8', errors='ignore') as gzf:
        with open(dest_filepath, "a+", encoding='utf-8', errors='ignore') as dest_file:
            for line in gzf:
                # replace tabs with spaces in each line
                tab_free_line = line.replace('\t', ' ')
                dest_file.write(tab_free_line)
            

def export_tar_file(tar_filepath, dest_filepath):
    """Function opens tar archive, read txt file and write its conntent to dest_filepath.
    If tar archive has no files or has multiple files then warning message is displayes"""
    
    with tarfile.open(tar_filepath, "r:gz") as tf:
        # get list of all files in archive ignoring directories
        tarfile_lst = [tarinfo.name for tarinfo in tf if tarinfo.isreg()]
        tarfile_lst = [filename for filename in tarfile_lst if not filename.endswith('.ss')]
        if not tarfile_lst:
            print('WARNING. No configuration in tar archive.')
        elif len(tarfile_lst) > 1:
            print('WARNING. Multiple files in tar archive.')
            print(tar_filepath)
            print(tarfile_lst)
        # read content of the first file
        tar_file_content = tf.extractfile(tarfile_lst[0]).read().decode("utf-8", errors='ignore')
        # write content to the dest_filepath
        with open(dest_filepath, "a+") as dest_file:
            for line in tar_file_content:
                # replace tabs with spaces in each line
                tab_free_line = line.replace('\t', ' ')
                dest_file.write(tab_free_line)


def export_single_section_file(input_filepath, output_filepath):
    """Function exports input_filepath (tar or gz with single txt file)
    to output_filepath"""
    
    # tar file export
    if tarfile.is_tarfile(input_filepath):
        export_tar_file(tar_filepath=input_filepath, dest_filepath=output_filepath)
    # gz file export
    else:
        export_gzip_file(gzip_filepath=input_filepath, dest_filepath=output_filepath)


def build_sshow_file(ssave_sections_stat_current_df, sshow_filepath):
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
    # insert footer to the end of the file
    insert_sshow_footer(sshow_filepath)
            

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
    report.add_log_entry(sshow_filepath, border_str, *str_lst[:3], border_str, *str_lst[3:], border_str, '')


def insert_section_header(sshow_filepath, section_name):
    """Function inserts sshow section title to concatenated sshow file"""
    
    section_header_str = "| Section: " + section_name + " |"
    border_str = "+" + "-" * (len(section_header_str) - 2) + "+"
    report.add_log_entry(sshow_filepath, border_str, section_header_str, border_str, '\n')


def insert_sshow_footer(sshow_filepath):
    """Function inserts sshow section title to concatenated sshow file"""
    
    footer_str = "| ... rebuilt finished |"
    border_str = "+" + "-" * (len(footer_str) - 2) + "+"
    report.add_log_entry(sshow_filepath, border_str, footer_str, border_str, '\n')