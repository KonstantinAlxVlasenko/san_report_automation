"""Module to download 3PAR configuratin files from STATs and 
extract configuaration information from downloaded and local files"""


import os
import re
import shutil
import subprocess
import sys

import pandas as pd

from common_operations_filesystem import (create_folder, find_files, load_data,
                                          save_data)
from common_operations_miscellaneous import (force_extract_check, line_to_list,
                                             reply_request, status_info,
                                             update_dct, verify_data)
from common_operations_servicefile import columns_import, data_extract_objects

S3MFT_DIR = r'C:\Users\vlasenko\Documents\02.DOCUMENTATION\Procedures\SAN Assessment\3par_stats\V5.0055\WINDOWS'
S3MFT = r's3mft.exe'


def storage_3par_extract(nsshow_df, nscamshow_df, local_3par_folder, project_folder, report_data_lst):
    """Function to extract 3PAR storage information"""
    
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['system_3par', 'port_3par', 'host_3par']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    system_3par_comprehensive_lst, port_3par_comprehensive_lst, host_3par_comprehensive_lst = data_lst

    # data force extract check 
    # list of keys for each data from data_lst representing if it is required 
    # to re-collect or re-analyze data even they were obtained on previous iterations
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # print data which were loaded but for which force extract flag is on
    force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # when any of data_lst was not saved or 
    # force extract flag is on then re-extract data  from configueation files
    if not all(data_lst) or any(force_extract_keys_lst):

        # lists to store only REQUIRED infromation
        # collecting data for all systems during looping
        # list containing system parameters
        system_3par_comprehensive_lst = []
        # list containing 3par FC port information
        port_3par_comprehensive_lst = []
        # list containing hosts defined on 3par ports
        host_3par_comprehensive_lst = []

        # data imported from init file to extract values from config file
        params, params_add, comp_keys, match_keys, comp_dct = data_extract_objects('3par', max_title)
        # verify if 3par systems registered in fabric NameServer
        ns_3par_df = verify_ns_3par(nsshow_df, nscamshow_df, comp_dct)

        if not ns_3par_df.empty:
            print('\n')
            print('3PAR Storage Systems detected in SAN')
            print(ns_3par_df)
            print('\n')
            # find configuration files to parse (download from STATs, local folder or use configurations
            # downloaded on previous iterations)
            configs_3par_lst = configs_download(ns_3par_df, project_folder, local_3par_folder, comp_dct, max_title)

            if configs_3par_lst:
                print('\nEXTRACTING 3PAR STORAGE INFORMATION ...\n')   
                # number of files to check
                configs_num = len(configs_3par_lst)  

                for i, config_3par in enumerate(configs_3par_lst):       
                    # file name
                    configname = os.path.basename(config_3par)
                    # current operation information string
                    info = f'[{i+1} of {configs_num}]: {configname} system'
                    print(info, end =" ")
                    showsys_lst, port_lst, host_lst = parse_config(config_3par, params, params_add, comp_keys, match_keys, comp_dct)
                    system_3par_comprehensive_lst.extend(showsys_lst)
                    port_3par_comprehensive_lst.extend(port_lst)
                    host_3par_comprehensive_lst.extend(host_lst)
                    if port_lst or host_lst:
                        status_info('ok', max_title, len(info))
                    else:
                        status_info('no data', max_title, len(info))

                # save extracted data to json file
                save_data(report_data_lst, data_names, system_3par_comprehensive_lst, port_3par_comprehensive_lst, host_3par_comprehensive_lst)
        else:
            # current operation information string
            info = f'Collecting 3PAR storage systems information'
            print(info, end =" ")
            status_info('skip', max_title, len(info))
            # save empty data to json file
            save_data(report_data_lst, data_names, system_3par_comprehensive_lst, port_3par_comprehensive_lst, host_3par_comprehensive_lst)
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        system_3par_comprehensive_lst, port_3par_comprehensive_lst, host_3par_comprehensive_lst = verify_data(report_data_lst, data_names, *data_lst)
    
    return system_3par_comprehensive_lst, port_3par_comprehensive_lst, host_3par_comprehensive_lst


def verify_ns_3par(nsshow_df, nscamshow_df, comp_dct):
    """Function to verify if 3PAR storage systems present in fabrics by checking
    local and cached Name Server DataFrames"""

    storage_columns = ['System_Model', 'Serial_Number']
    ns_lst = [nsshow_df, nscamshow_df]
    ns_3par_lst = []

    for ns_df in ns_lst:
        ns_3par_df = ns_df.copy()
        # extract 3par records
        ns_3par_df[storage_columns] =  ns_3par_df['NodeSymb'].str.extract(comp_dct['ns_3par'])
        ns_3par_df = ns_3par_df[storage_columns].copy()
        ns_3par_df.dropna(subset=['Serial_Number'], inplace=True)
        ns_3par_lst.append(ns_3par_df)
        
    ns_3par_df, nscam_3par_df = ns_3par_lst
    # concatenate local and cached NS 3PAR records
    ns_3par_df = pd.concat([ns_3par_df, nscam_3par_df])
    ns_3par_df.drop_duplicates(inplace=True)
    ns_3par_df.reset_index(drop=True, inplace=True)
    return ns_3par_df


def configs_download(ns_3par_df, project_folder, local_3par_folder, comp_dct, max_title):
    """Function to prepare 3PAR configuration files for parsing. 
    Download in from STATs and local (defined in report.xlsx file) folders"""

    # folder for 3par config files download is in project folder
    download_folder = os.path.join(project_folder, '3par_configs')
    # verify if download folder exist (created on previous iterations)
    download_folder_exist = verify_download_folder(download_folder, create=False)

    # if downlad folder exist and there are 3par config files
    # ask if user wants to use them or download new files after removing existing
    if download_folder_exist:
        configs_downloaded_lst = find_files(download_folder, max_title, filename_contains=comp_dct['configname_3par'])
        if configs_downloaded_lst:
            print('\n')
            print(f'{len(configs_downloaded_lst)} 3PAR configuration files found in download folder.')
            print('Do you want to USE EXISTING FILES to extract port and host information?')
            print("If you want to update configuarion files and reply 'no' then existing files are removed.")
            query = "Please select '(y)es' (use existing) or '(n)o' (update): "
            reply = reply_request(query)
            if reply == 'n':
                # delete 3par config files
                remove_files(configs_downloaded_lst, max_title)
            else:
                return configs_downloaded_lst
    
    # download from STATs
    print('\n')
    query = 'Do you want DOWNLOAD configuration files from STATs? (y)es/(n)o: '
    reply = reply_request(query)
    if reply == 'y':
        query = 'Are you connected to HPE network?: '
        reply = reply_request(query)
        if reply == 'y':
            # download configs from STATs
            stats_download(ns_3par_df, download_folder, max_title)
        else:
            print('STATs is only available within HPE network.')

    # find configuration files in local 3PAR folder
    if local_3par_folder:
        configs_local_lst = find_files(local_3par_folder, max_title, filename_contains=comp_dct['configname_3par'])
        if configs_local_lst:
            print('\n')
            print(f'{len(configs_local_lst)} 3PAR configuration files found in local folder.')
            query = 'Do you want COPY configuration files from LOCAL to download folder? (y)es/(n)o: '
            reply = reply_request(query)
            if reply == 'y':
                # download configs from loacl folder
                local_download(configs_local_lst, download_folder, max_title)

    # create configs list in download folder if it exist (if user reject download config files
    # from both sources download folder is not created)
    download_folder_exist = verify_download_folder(download_folder, create=False)
    if download_folder_exist:
        configs_3par_lst = find_files(download_folder, max_title, filename_contains=comp_dct['configname_3par'])
        return configs_3par_lst
    else:
        return []


def verify_download_folder(download_3par_folder, max_title=80, create=True):
    """Function to verify if folder with downloaded configs exist. 
    If not then create it if create key is True"""
    
    folder_exist = os.path.isdir(download_3par_folder)

    if not folder_exist and create:
        create_folder(download_3par_folder, max_title)
        return True
    return folder_exist


def remove_files(files_lst, max_title):
    """Function to remove files from the list"""

    print('\n')
    for i, file in enumerate(files_lst):
        filename = os.path.basename(file)
        info = ' '*16 + f'[{i+1} of {len(files_lst)}]: Removing file {filename}'
        print(info, end=' ')
        # verify if file exist
        if os.path.isfile(file) or os.path.islink(file):
            try:
                os.remove(file)
                status_info('ok', max_title, len(info))
            except OSError as e:
                status_info('failed', max_title, len(info))
                print('\n')
                print(e.strerror)
        else:
            status_info('skip', max_title, len(info))


def stats_download(ns_3par_df, download_folder, max_title):
    """Function to download 3PAR configuration files from STATs with
    help of s3mft program"""

    # verifu if download folder exist and create one if not (default behaviour)
    verify_download_folder(download_folder, max_title)
    # list of serial numbers and models
    sn_lst = ns_3par_df['Serial_Number'].tolist()
    model_lst = ns_3par_df['System_Model'].tolist()

    s3mft_path = os.path.join(S3MFT_DIR, S3MFT)
    s3mft_path = os.path.normpath(s3mft_path)
    print('\n')

    for i, (model, sn) in enumerate(zip(model_lst, sn_lst)):
        info = ' '*16 + f'[{i+1} of {len(sn_lst)}]: Downloading config for {model} {sn}'
        print(info, end=" ")
        try:
            # request for latest 3par config file
            config = subprocess.check_output(f'"{s3mft_path}" -n {sn} -p config -stlatest', shell=True, text=True)
            if config:
                config = config.strip('\n')
                # download config if it's exist
                run =  subprocess.run(fr'"{s3mft_path}" -filename "{config}" -fnp -fo -outdir "{download_folder}" -quiet', shell=True)
                # verify if file exist (downloaded) and return corresponnding status
                config_filename = os.path.basename(config)
                config_filename = 'array_' + sn + '_' + config_filename
                config_file = os.path.join(download_folder, config_filename)        
                if not run.returncode and os.path.isfile(config_file):
                    status_info('ok', max_title, len(info))
                else:
                    status_info('fail', max_title, len(info))
            else:
                status_info('skip', max_title, len(info))
        # if s3mft was not able to retreive config filename
        except subprocess.CalledProcessError as e:
            status_info('fail', max_title, len(info))
            print('\n')
            print('WARNING!!! Check HPE network connection.')
            print(e)
            sys.exit()


def local_download(configs_local_lst, download_folder, max_title):
    """Function to copy 3PAR configuration files from local folder
    to download folder"""

    # verifu if download folder exist and create one if not (default behaviour)
    verify_download_folder(download_folder, max_title)

    print('\n')
    # copy all files from configs_local_lst to download folder
    for i, source_file in enumerate(configs_local_lst):
        filename = os.path.basename(source_file)
        info = ' '*16 + f'[{i+1} of {len(configs_local_lst)}]: copying file {filename}'
        print(info, end=' ')
        if os.path.isfile(source_file) or os.path.islink(source_file):
            # destination_file = os.path.join(download_folder, filename)
            try:
                shutil.copy2(source_file, download_folder)
                status_info('ok', max_title, len(info))
            except shutil.Error as e:
                status_info('failed', max_title, len(info))
                print('\n')
                print(e)
        else:
            status_info('skip', max_title, len(info))


def parse_config(config_3par, params, params_add, comp_keys, match_keys, comp_dct):
    """Function to parse 3PAR config file""" 

    # file name
    configname = os.path.basename(config_3par)
    # search control dictionary. continue to check file until all parameters groups are found
    collected = {'system': False, 'ip': False, 'port': False, 'host': False}

    # initialize structures to store collected data for current storage
    # dictionary to store all DISCOVERED parameters
    showsys_dct = {}
    showsys_lst = []
    # if lists remains empty after file parsing than status_info shows NO_DATA for current file
    port_lst = []
    host_lst = []
    # Storage IP address
    ip_addr = None
    
    with open(config_3par, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()
            if not line:
                break
            # showsys section start
            if re.search(comp_dct['showsys_header'], line) and not collected['system']:
                collected['system'] = True
                # while not reach empty line
                while not re.search(comp_dct['section_end'],line):
                    line = file.readline()
                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                    # name_value_pair_match
                    if match_dct['parameter_value_pair']:
                        result = match_dct['parameter_value_pair']
                        showsys_dct[result.group(1).strip()] = result.group(2).strip()  
                    if not line:
                        break
            # showsys section end
            # port section start
            elif re.search(comp_dct['showport_header'], line) and not collected['port']:
                collected['port'] = True
                while not re.search(comp_dct['section_end'], line):
                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                    # port_line_match
                    if match_dct['port_line']:
                        port_line = line_to_list(comp_dct['port_line'], line, configname)
                        port_lst.append(port_line)
                    line = file.readline()
                    if not line:
                        break
            # port section end
            # host section start
            elif re.search(comp_dct['showhost_header'], line) and not collected['host']:
                collected['host'] = True
                while not re.search(comp_dct['section_end'], line):
                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                    # port_line_match
                    if match_dct['host_line']:
                        host_line = line_to_list(comp_dct['host_line'], line, configname)
                        host_lst.append(host_line)
                    line = file.readline()
                    if not line:
                        break
            # host section end
            # ip_address section start
            elif re.search(comp_dct['ip_address'], line) and not collected['ip']:
                collected['ip'] = True
                ip_addr = re.search(comp_dct['ip_address'], line).group(1)
            # ip_address section end

    # additional values which need to be added to the switch params dictionary 
    # switch_params_add order ('configname', 'chassis_name', 'switch_index', 'ls_mode')
    # values axtracted in manual mode. if change values order change keys order in init.xlsx switch tab "params_add" column
    showsys_values = (configname, ip_addr)

    if showsys_dct:
        # adding additional parameters and values to the parameters dct
        update_dct(params_add, showsys_values, showsys_dct)                                                
        # creating list with REQUIRED parameters for the current system.
        # if no value in the dct for the parameter then None is added 
        # and appending this list to the list of all systems     
        showsys_lst.append([showsys_dct.get(param) for param in params])

    return showsys_lst, port_lst, host_lst
