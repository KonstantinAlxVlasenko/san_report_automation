"""Auxiliary module for collection_3par module to download 3PAR configuration files from STATs or Local folder"""

import os
import re
import shutil
import subprocess
from datetime import date, timedelta

import numpy as np
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.filesystem_operations as fsop



# from common_operations_filesystem import create_folder, find_files
# from common_operations_miscellaneous import reply_request, status_info
# from common_operations_table_report import dataframe_to_report


# S3MFT_DIR = r'C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\02.DOCUMENTATION\Procedures\SAN Assessment\3par_stats\V5.0120\WINDOWS'
# S3MFT = r's3mft.exe'


def configs_download(ns_3par_df, pattern_dct, project_constants_lst, software_path_sr):
    """Function to prepare 3PAR configuration files for parsing. 
    Download in from STATs and local (defined in report.xlsx file) folders"""


    # # report_steps_dct contains current step desciption and force and export tags
    # report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst, *_ = report_creation_info_lst
    # *_, max_title = report_constant_lst

    # imported project constants required for module execution
    _, max_title, _, report_requisites_sr, *_ = project_constants_lst

    project_folder = report_requisites_sr['project_folder']
    local_3par_folder = report_requisites_sr['3par_inserv_folder']

    # folder for 3par config files download is in project folder
    download_folder = os.path.join(project_folder, '3par_configs')
    # verify if download folder exist (created on previous iterations)
    download_folder_exist = verify_download_folder(download_folder, create=False)

    # if download folder exist and there are 3par config files
    # ask if user wants to use them or download new files after removing existing
    if download_folder_exist:
        configs_downloaded_lst = fsop.find_files(download_folder, max_title, filename_contains=pattern_dct['configname_3par'])
        if configs_downloaded_lst:
            print(f'{len(configs_downloaded_lst)} 3PAR configuration files found in download folder.')
            print("Do you want to USE EXISTING configuarion files? (reply 'YES')")
            print("If you want to REMOVE EXISTING AND UPDATE configuarion files then reply 'NO'.")
            query = "Please select '(y)es' (use existing) or '(n)o' (update): "
            reply = meop.reply_request(query)
            if reply == 'n':
                # delete 3par config files
                print('\n')
                remove_files(configs_downloaded_lst, max_title)
            else:
                return configs_downloaded_lst
    
    # download from STATsS
    query = 'Do you want DOWNLOAD configuration files from STATs? (y)es/(n)o: '
    reply = meop.reply_request(query)
    if reply == 'y':
        query = 'Are you connected to HPE network?: '
        reply = meop.reply_request(query)
        if reply == 'y':
            # download configs from STATs
            ns_3par_df = stats_download(ns_3par_df, download_folder, software_path_sr, max_title)
        else:
            print('STATs is only available within HPE network.')

    # find configuration files in local 3PAR folder
    if local_3par_folder:
        configs_local_lst = fsop.find_files(local_3par_folder, max_title, filename_contains=pattern_dct['configname_3par'])
        if configs_local_lst:
            print('\n')
            print(f'{len(configs_local_lst)} 3PAR configuration files found in local folder.')
            query = 'Do you want COPY configuration files from LOCAL to download folder? (y)es/(n)o: '
            reply = meop.reply_request(query)
            if reply == 'y':
                # download configs from loacl folder
                ns_3par_df = local_download(ns_3par_df, configs_local_lst, download_folder, pattern_dct, max_title)

    download_summary(ns_3par_df, project_constants_lst)
    # create configs list in download folder if it exist (if user reject download config files
    # from both sources download folder is not created)
    download_folder_exist = verify_download_folder(download_folder, create=False)
    if download_folder_exist:
        configs_3par_lst = fsop.find_files(download_folder, max_title, filename_contains=pattern_dct['configname_3par'])
        return configs_3par_lst
    else:
        return []


def verify_download_folder(download_3par_folder, max_title=80, create=True):
    """Function to verify if folder with downloaded configs exist. 
    If not then create it if create key is True"""
    
    folder_exist = os.path.isdir(download_3par_folder)

    if not folder_exist and create:
        fsop.create_folder(download_3par_folder, max_title)
        return True
    return folder_exist


def remove_files(files_lst, max_title):
    """Function to remove files from the list"""

    for i, file in enumerate(files_lst):
        filename = os.path.basename(file)
        info = ' '*16 + f'[{i+1} of {len(files_lst)}]: Removing file {filename}'
        print(info, end=' ')
        # verify if file exist
        if os.path.isfile(file) or os.path.islink(file):
            try:
                os.remove(file)
                meop.status_info('ok', max_title, len(info))
            except OSError as e:
                meop.status_info('failed', max_title, len(info))
                print('\n')
                print(e.strerror)
        else:
            meop.status_info('skip', max_title, len(info))
    print('\n')


def stats_download(ns_3par_df, download_folder, software_path_sr, max_title):
    """Function to download 3PAR configuration files from STATs with
    help of s3mft program"""

    # verify if download folder exist and create one if not (default behaviour)
    verify_download_folder(download_folder, max_title)
    # list of serial numbers and models
    sn_lst = ns_3par_df['Serial_Number'].tolist()
    model_lst = ns_3par_df['System_Model'].tolist()

    # s3mft_path = os.path.join(S3MFT_DIR, S3MFT)
    # s3mft_path = os.path.normpath(s3mft_path)
    s3mft_path = software_path_sr['s3mft']

    today = date.today().strftime("%y%m%d")
    yesterday = (date.today() - timedelta(1)).strftime("%y%m%d")

    print('\n')

    for i, (model, sn) in enumerate(zip(model_lst, sn_lst)):
        info = ' '*16 + f'[{i+1} of {len(sn_lst)}]: Downloading config for {model} {sn}'
        print(info, end=" ")

        # request for latest 3par config file
        # config_str = subprocess.check_output(f'"{s3mft_path}" -n {sn} -p config -stlatest', shell=True, text=True)
        output = subprocess.run(f'"{s3mft_path}" -n {sn} -stlatest -filetype config -quiet', text=True, capture_output=True)

        # if s3mft was not able to retreive config filename
        if output.returncode:
            if 'no data found' in output.stderr:
                config = None
                status = meop.status_info('no data', max_title, len(info))
            else:
                meop.status_info('fail', max_title, len(info))
                print(output.stderr)
                exit()
        else:
            output_str = output.stdout.strip('\n')
            *_, config = output_str.split('\n')

            # download configs within one day only
            if today in config or yesterday in config:
                # download config if it's exist
                download =  subprocess.run(fr'"{s3mft_path}" -filename "{config}" -fnp -fo -outdir "{download_folder}" -quiet', shell=True, capture_output=True)
                # verify if file exist (downloaded) and return corresponnding status
                config_filename = os.path.basename(config)
                config_filename = 'array_' + sn + '_' + config_filename
                config_file = os.path.join(download_folder, config_filename)        
                if not download.returncode and os.path.isfile(config_file):
                    status = meop.status_info('ok', max_title, len(info))
                else:
                    status = meop.status_info('fail', max_title, len(info))
            else:
                status = meop.status_info('skip', max_title, len(info))
        
        ns_3par_df.loc[i, ['configname', 'STATs_status']] = [config, status.lower()]
    return ns_3par_df


def local_download(ns_3par_df, configs_local_lst, download_folder, pattern_dct, max_title):
    """Function to copy 3PAR configuration files from local folder
    to download folder"""

    # verifu if download folder exist and create one if not (default behaviour)
    verify_download_folder(download_folder, max_title)

    if not 'STATs_status' in ns_3par_df.columns:
        ns_3par_df['STATs_status'] = np.nan
        drop_stats_column = True
    else:
        drop_stats_column = False

    ns_3par_df[['filename', 'Local_status']] = np.nan

    sn_lst = ns_3par_df['Serial_Number'].tolist()

    print('\n')
    # copy files from configs_local_lst to download folder
    for i, source_file in enumerate(configs_local_lst):
        filename = os.path.basename(source_file)
        if os.path.isfile(source_file) or os.path.islink(source_file):
            # extract model and seral number from config file
            model, sn = parse_serial(source_file, pattern_dct)
            info = ' '*16 + f'[{i+1} of {len(configs_local_lst)}]: Copying {filename} for {model} {sn}'
            print(info, end=' ')
            # verify if config for current sn was downloaded from STATs or local folder before
            mask_sn = ns_3par_df['Serial_Number'] == sn
            status_ok = (ns_3par_df.loc[mask_sn, ['STATs_status', 'Local_status']] == 'ok').any().any()
            # copy config if 3par is in NameServer and was not downloaded from STATs or local folder before
            if sn in sn_lst and not status_ok:
                # destination_file = os.path.join(download_folder, filename)
                status = 'unknown'
                try:
                    shutil.copy2(source_file, download_folder)
                    status = meop.status_info('ok', max_title, len(info))
                except shutil.Error as e:
                    status = meop.status_info('failed', max_title, len(info))
                    print('\n')
                    print(e)
                finally:
                    ns_3par_df.loc[mask_sn, ['filename', 'Local_status']] = [filename, status.lower()]
            else:
                status = meop.status_info('skip', max_title, len(info))
        else:
            info = ' '*16 + f'[{i+1} of {len(configs_local_lst)}]: copying {filename}'
            print(info, end=' ')
            meop.status_info('skip', max_title, len(info))
    
    if drop_stats_column:
        ns_3par_df.drop(columns=['STATs_status'], inplace=True)

    return ns_3par_df


def parse_serial(config_3par, pattern_dct):
    """Function to parse 3PAR serial number""" 

    # search control dictionary. continue to check file until all parameters groups are found
    collected = {'system': False}
    sn = None
    model = None
    
    with open(config_3par, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()
            if not line:
                break
            # showsys section start
            if re.search(pattern_dct['showsys_header'], line) and not collected['system']:
                collected['system'] = True
                # while not reach empty line
                while not re.search(pattern_dct['section_end'],line):
                    line = file.readline()
                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # name_value_pair_match
                    if match_dct['serial_number']:
                        result = match_dct['serial_number']
                        sn = result.group(1).strip()
                    elif match_dct['system_model']:
                        result = match_dct['system_model']
                        model = result.group(1).strip()
                    if not line:
                        break
            # showsys section end
    return model, sn


def download_summary(ns_3par_df, project_constants_lst):
    """Function to print configurations download from STATs summary and
    save summary to file if user agreed"""

    if not 'STATs_status' in ns_3par_df.columns and \
        not 'Local_status' in ns_3par_df.columns:
        ns_3par_df['Status'] = 'skip'

    print('\n')
    print('3PAR Storage Systems configuaration download summary\n')
    print(ns_3par_df)
    print('\n')

    query = 'Do you want to SAVE download SUMMARY? (y)es/(n)o: '
    reply = meop.reply_request(query)
    if reply == 'y':
        dfop.dataframe_to_excel(ns_3par_df, 'stats_summary', project_constants_lst, force_flag=True)



