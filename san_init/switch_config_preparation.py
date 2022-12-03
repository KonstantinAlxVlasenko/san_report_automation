import os
import re
import shutil
import sys

import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .santoolbox_parser import santoolbox_process


def switch_config_preprocessing(project_constants_lst, software_path_sr):
    

    _, max_title, _, report_requisites_sr, *_ = project_constants_lst

    ssave_folder = report_requisites_sr['supportsave_folder']
    parsed_sshow_folder = report_requisites_sr['parsed_sshow_folder']
    parsed_other_folder = report_requisites_sr['parsed_other_folder']


    # check for switches unparsed configuration data
    # returns list with config data file paths (ssave, amsmaps) 
    unparsed_sshow_maps_lst = create_files_list_to_parse(ssave_folder, max_title)


    # export unparsed config filenames to DataFrame and saves it to report file and database
    # unparsed_sshow_maps_df = dfop.list_to_dataframe(unparsed_sshow_maps_lst, max_title, columns=['sshow', 'ams_maps'])
    unparsed_sshow_maps_df, *_ = dfop.list_to_dataframe(['sshow', 'ams_maps'], unparsed_sshow_maps_lst)
    # returns list with parsed data
    parsed_sshow_maps_lst, parsed_sshow_maps_filename_lst = santoolbox_process(unparsed_sshow_maps_lst, 
                                                                                parsed_sshow_folder, parsed_other_folder, software_path_sr, max_title)
    # export parsed config filenames to DataFrame and saves it to excel file
    parsed_sshow_maps_df, *_ = dfop.list_to_dataframe(['chassis_name', 'sshow', 'ams_maps'], parsed_sshow_maps_filename_lst)
                                    
    # save files list to database and excel file
    data_names = ['unparsed_files', 'parsed_files']
    data_lst = [unparsed_sshow_maps_df, parsed_sshow_maps_df]
    for df in data_lst:
        df['ams_maps'] = df['ams_maps'].astype('str')
        df['ams_maps'] = df['ams_maps'].str.strip('[]()')

    dbop.write_database(project_constants_lst, data_names, *data_lst)
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)    
    return parsed_sshow_maps_lst


def create_files_list_to_parse(ssave_path, max_title):
    """
    Function to create two lists with unparsed supportshow and amps_maps configs data files.
    Directors have two ".SSHOW_SYS.txt.gz" files. For Active and Standby CPs
    Configuration file for Active CP has bigger size
    """
    
    print(f'\n\nPREREQUISITES 3. SEARCHING SUPPORSAVE CONFIGURATION FILES\n')
    print(f'Configuration data folder {ssave_path}')

    # check if ssave_path folder exist
    fsop.check_valid_path(ssave_path)
    # rellocate files for each switch in separate folder
    separate_ssave_files(ssave_path, max_title)
   
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

            elif file.endswith("AMS_MAPS_LOG.txt.gz") or file.endswith("AMS_MAPS_LOG.tar.gz"):
                ams_maps_num += 1
                ams_maps_file_path = os.path.normpath(os.path.join(root, file))
                ams_maps_files_lst_tmp.append(ams_maps_file_path)
                filename_size.append(len(file))
        
        # add info to unparsed list only if supportshow file has been found in current directory
        # if supportshow found but there is no ams_maps files then empty ams_maps list appended to config set 
        if sshow_file_path:
            unparsed_files_lst.append([sshow_file_path, tuple(ams_maps_files_lst_tmp)])
            
    sshow_num = len(unparsed_files_lst)
    print(f'SSHOW_SYS: {sshow_num}, AMS_MAPS_LOG: {ams_maps_num}, Total: {sshow_num + ams_maps_num} configuration files.')
    
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
                fsop.create_folder(files_group_folder, max_title)
                
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
                    meop.status_info('fail', max_title, len(info))
                    sys.exit()
                else:
                    meop.status_info('ok', max_title, len(info))
