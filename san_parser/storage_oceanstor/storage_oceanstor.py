"""Module to extract Huawei OceanStore information"""

import os

import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.report_operations as report
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop

from .storage_oceanstor_extract import *


def storage_oceanstor_extract(project_constants_lst):
    """Function to extract Huawei OceanStor storage information"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, report_requisites_sr, *_ = project_constants_lst
    hw_oceanstor_folder = report_requisites_sr['huawei_oceanstor_folder']

    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'storage_oceanstor_collection')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)

    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data 
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)
    
    if force_run:
        # system summary details
        san_system_oceanstor_lst = []
        # system fc ports
        san_fcport_oceanstor_lst = []
        # hosts defined in storage configuration
        san_host_oceanstor_lst = []
        # host names defined in storage configuration
        san_host_id_name_oceanstor_lst = []
        # host fc ports defined in storage configuration
        san_host_id_fcinitiator_oceanstor_lst = []
        # extracted storage configs (sn: configname) to avoid duplicates
        san_extracted_oceanstor_dct = {}
        # extracted relation hostid, controller portid and lunid
        san_hostid_ctrlportid_oceanstor_lst = []
        
        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('oceanstor', max_title)
        system_params, system_params_add, fcport_params, host_params, hostid_name_params = dfop.list_from_dataframe(
            re_pattern_df, 'system_params', 'system_params_add', 'fcport_params', 'host_params', 'hostid_name_params')

        if hw_oceanstor_folder:    
            print('\nEXTRACTING HUAWEI OCEANSTOR STORAGE INFORMATION ...\n')  
            oceanstor_configs_lst = fsop.find_files(hw_oceanstor_folder, max_title, filename_extension='txt')
            configs_num = len(oceanstor_configs_lst)
        
            if configs_num:
                for i, storage_config in enumerate(oceanstor_configs_lst):       
                    # file name with extension
                    configname_wext = os.path.basename(storage_config)
                    # remove extension from filename
                    configname, _ = os.path.splitext(configname_wext)
                    # current operation information string
                    info = f'[{i+1} of {configs_num}]: {configname} file.'
                    print(info, end =" ")
                    # extract params from the current config file
                    storage_oceanstore_lst, info, duplicated_configname  = storage_params_extract(storage_config, san_extracted_oceanstor_dct,
                                                                                                    system_params, system_params_add, fcport_params, host_params, hostid_name_params,
                                                                                                    pattern_dct, info)                    
                    # add current storage config data to the total storage configs
                    for current_storage_lst, san_storage_lst in zip(storage_oceanstore_lst, 
                                                                    [san_system_oceanstor_lst, san_fcport_oceanstor_lst, san_host_oceanstor_lst,
                                                                     san_host_id_name_oceanstor_lst, san_host_id_fcinitiator_oceanstor_lst,
                                                                     san_hostid_ctrlportid_oceanstor_lst]):
                        if current_storage_lst:
                            san_storage_lst.extend(current_storage_lst)
                    
                    if duplicated_configname:
                        meop.status_info('skip', max_title, len(info))
                        print(f'Note. Duplication with extracted config {duplicated_configname}')
                        print('\n')
                    else:
                        # show status if any configuration data is collected
                        meop.show_collection_status(storage_oceanstore_lst, max_title, len(info))
        else:
            # current operation information string
            info = f'Collecting OceanStor storage systems information'
            print(info, end =" ")
            meop.status_info('skip', max_title, len(info))
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 
                                               'system_columns', 'fcport_columns', 'host_columns', 
                                               'hostid_name_columns', 'hostid_fcinitiator_columns',
                                               'hostid_ctrlport_relation_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, 
                                          san_system_oceanstor_lst, san_fcport_oceanstor_lst, san_host_oceanstor_lst, 
                                          san_host_id_name_oceanstor_lst, san_host_id_fcinitiator_oceanstor_lst, 
                                          san_hostid_ctrlportid_oceanstor_lst)
        san_system_oceanstor_df, san_fcport_oceanstor_df, san_host_oceanstor_df, \
            san_host_id_name_oceanstor_df, san_host_id_fcinitiator_oceanstor_df, san_hostid_ctrlportid_oceanstor_df, *_ = data_lst 
        
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst) 
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        san_system_oceanstor_df, san_fcport_oceanstor_df, san_host_oceanstor_df, \
            san_host_id_name_oceanstor_df, san_host_id_fcinitiator_oceanstor_df, san_hostid_ctrlportid_oceanstor_df = data_lst
    
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst)            
    return san_system_oceanstor_df, san_fcport_oceanstor_df, san_host_oceanstor_df, \
        san_host_id_name_oceanstor_df, san_host_id_fcinitiator_oceanstor_df, san_hostid_ctrlportid_oceanstor_df
            