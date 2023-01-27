"""Module to create storage hosts DataFrame"""

import numpy as np
import pandas as pd


import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
# import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop


def storage_host_analysis(host_3par_df, system_3par_df, port_3par_df, 
                                portshow_aggregated_df, zoning_aggregated_df, 
                                project_constants_lst):
    """Main function to analyze storage port configuration"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, _, report_headers_df, report_columns_usage_sr, *_ = project_constants_lst

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'storage_host_analysis_out', 'storage_host_analysis_in')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating storage hosts table'
        print(info, end =" ") 
        
        storage_host_aggregated_df = \
            storage_host_aggregation(host_3par_df, system_3par_df, port_3par_df, portshow_aggregated_df, zoning_aggregated_df)
        # after finish display status
        meop.status_info('ok', max_title, len(info))
        # report tables
        storage_host_report_df, storage_host_compare_report_df = \
            storage_host_report(storage_host_aggregated_df, data_names, report_headers_df, report_columns_usage_sr)
        # create list with partitioned DataFrames
        data_lst = [storage_host_aggregated_df, storage_host_report_df, storage_host_compare_report_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        storage_host_aggregated_df, *_ = data_lst
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return storage_host_aggregated_df


def storage_host_aggregation(host_3par_df, system_3par_df, port_3par_df, portshow_aggregated_df, zoning_aggregated_df):
    """Function to create aggregated storage host presentation DataFrame"""

    if system_3par_df.empty:
        return pd.DataFrame()
    
    storage_host_aggregated_df = host_3par_df.copy()
    # add system_name
    storage_host_aggregated_df = dfop.dataframe_fillna(storage_host_aggregated_df, system_3par_df, 
                                                    join_lst=['configname'], filled_lst=['System_Name'])
    # add controller's ports Wwnp and Wwnp
    storage_host_aggregated_df = dfop.dataframe_fillna(storage_host_aggregated_df, port_3par_df, 
                                                    join_lst=['configname', 'Storage_Port'], filled_lst=['NodeName', 'PortName'])
    # convert Wwnn and Wwnp to regular represenatation (lower case with colon delimeter)
    storage_host_aggregated_df = dfop.convert_wwn(storage_host_aggregated_df, ['Host_Wwn', 'NodeName', 'PortName'])
    # add controllers ports Fabric_name and Fabric_label
    storage_host_aggregated_df = dfop.dataframe_fillna(storage_host_aggregated_df, portshow_aggregated_df, 
                                                    join_lst=['PortName'], filled_lst=['Fabric_name', 'Fabric_label'])
    # rename controllers NodeName and PortName
    rename_columns = {'NodeName': 'Storage_Port_Wwnn', 'PortName': 'Storage_Port_Wwnp'}
    storage_host_aggregated_df.rename(columns=rename_columns, inplace=bool)
    # 'clean' Wwn column to have Wwnp only. check Wwnn -> Wwnp correspondance in all fabrics
    storage_host_aggregated_df = dfop.replace_wwnn(storage_host_aggregated_df, 'Host_Wwn', 
                                                portshow_aggregated_df, ['NodeName', 'PortName'])
    # add Host Wwnp zoning device status in fabric of storage port connection
    storage_host_aggregated_df = dfop.dataframe_fillna(storage_host_aggregated_df, zoning_aggregated_df, 
                                                join_lst=['Fabric_name', 'Fabric_label', 'PortName'], 
                                                filled_lst=['Fabric_device_status'])
    # rename controllers Fabric_name and Fabric_label
    rename_columns = {'Fabric_name': 'Storage_Fabric_name', 'Fabric_label': 'Storage_Fabric_label', 
                        'Fabric_device_status': 'Fabric_host_status'}
    storage_host_aggregated_df.rename(columns=rename_columns, inplace=bool)
    # add host information
    host_columns = ['Fabric_name', 'Fabric_label', 'chassis_name', 'switchName', 
                    'Index_slot_port', 'portIndex', 'slot', 'port',  'Connected_portId', 
                    'Device_Host_Name', 'Device_Port', 'Host_OS', 'Device_Location', 
                    'Device_Host_Name_per_fabric_name_and_label',	'Device_Host_Name_per_fabric_label', 
                    'Device_Host_Name_per_fabric_name', 'Device_Host_Name_total_fabrics']
    storage_host_aggregated_df = dfop.dataframe_fillna(storage_host_aggregated_df, portshow_aggregated_df, 
                                                    join_lst=['PortName'], filled_lst=host_columns, remove_duplicates=False)
    # rename host columns
    rename_columns = {'Fabric_name': 'Host_Fabric_name', 'Fabric_label': 'Host_Fabric_label', 'PortName': 'Host_Wwnp'}
    storage_host_aggregated_df.rename(columns=rename_columns, inplace=bool)
    # verify if host and storage ports are in the same fabric
    storage_host_aggregated_df = dfop.sequential_equality_note(storage_host_aggregated_df, 
                                                            ['Host_Fabric_name', 'Host_Fabric_label'], 
                                                            ['Storage_Fabric_name', 'Storage_Fabric_label'],
                                                            'Host_Storage_Fabric_equal')
    # verify persona (host mode) is defined in coreespondence with host os
    storage_host_aggregated_df = verify_host_mode(storage_host_aggregated_df)
    # verify if storage port and host port are zoned
    storage_host_aggregated_df = verify_storage_host_zoning(storage_host_aggregated_df, zoning_aggregated_df)
    # sort aggregated DataFrame
    sort_columns = ['System_Name', 'Host_Id', 'Host_Name', 'Storage_Port']
    storage_host_aggregated_df.sort_values(by=sort_columns, inplace=True)
    # create storage name column free of duplicates
    storage_host_aggregated_df = dfop.remove_duplicates_from_column(storage_host_aggregated_df, 'System_Name',
                                                                duplicates_subset=['configname', 'System_Name'], ) 
    return storage_host_aggregated_df


def verify_host_mode(storage_host_aggregated_df):
    """Function to verify if persona (storage host mode) is defined in correspondence with host os"""

    os_lst = ['vmware', 'windows', 'linux']
    # cumulative host mode mask
    mask_persona_correct = None
    for os_type in os_lst:
        # host mode matches os name except for linux
        os_mode = os_type if os_type != 'linux' else 'generic'
        # mask for current os
        mask_os = (storage_host_aggregated_df['Persona'].str.lower().str.contains(os_mode) & \
                    storage_host_aggregated_df['Host_OS'].str.lower().str.contains(os_type))
        # add current mask to cumulative mask
        if mask_persona_correct is None:
            mask_persona_correct = mask_os
        else:
            mask_persona_correct = mask_persona_correct | mask_os

    # perform checking for rows with existing data only both in Persona and Host_OS columns
    mask_os_notna = storage_host_aggregated_df[['Persona', 'Host_OS']].notna().all(axis=1)
    storage_host_aggregated_df['Persona_correct'] = \
        np.select([mask_os_notna & mask_persona_correct, mask_os_notna & ~mask_persona_correct], ['Yes', 'No'], default=pd.NA)
    # replace pd.NA values
    storage_host_aggregated_df.fillna(np.nan, inplace=True)
    
    return storage_host_aggregated_df


def verify_storage_host_zoning(storage_host_aggregated_df, zoning_aggregated_df):
    """Function to verify if storage port and host port are zoned"""
    
    # prepare zoning (slice effective zoning and local or imported ports only)
    mask_connected = zoning_aggregated_df['Fabric_device_status'].isin(['local', 'remote_imported'])
    mask_effective = zoning_aggregated_df['cfg_type'] == 'effective'
    zoning_valid_df = zoning_aggregated_df.loc[mask_effective & mask_connected].copy()
    # find zones with 3PAR storages only to reduce search time
    group_columns = ['Fabric_name', 'Fabric_label', 'zone']
    zone_3par_df = \
        zoning_valid_df.groupby(by=group_columns).filter(lambda zone: zone['deviceSubtype'].str.lower().isin(['3par']).any())

    storage_host_aggregated_df['zone'] = \
        storage_host_aggregated_df.apply(lambda series: find_zones(series, zone_3par_df), axis=1)

    return storage_host_aggregated_df


def find_zones(series, zoning_valid_df):
    """Auxiliary function for verify_storage_host_zoning fn 
    to find zones in effective configuration with storage port and server"""
    
    # verify rows where storage port and server are in same fabric only
    if series['Fabric_host_status'] in  ['local', 'remote_imported']: 
        group_columns = ['Fabric_name', 'Fabric_label', 'zone']
        storage_host_sr = series[['Storage_Port_Wwnp', 'Host_Wwnp']]

        # excessive step zoning_valid_df is already filtered TO_REMOVE 
        mask_same_fabic = (zoning_valid_df['Fabric_name'] == series['Storage_Fabric_name']) & \
                            (zoning_valid_df['Fabric_label'] == series['Storage_Fabric_label'])
        zoning_valid_fabric_df = zoning_valid_df.loc[mask_same_fabic].copy()
        
        # find zones with storage port wwnp and host wwnp
        storage_host_zone_df = \
            zoning_valid_fabric_df.groupby(by=group_columns).filter(lambda zone: storage_host_sr.isin(zone['PortName']).all())
        # get zones defined in the same fabric as storage port connection fabric
        mask_same_fabic = (storage_host_zone_df['Fabric_name'] == series['Storage_Fabric_name']) & \
                            (storage_host_zone_df['Fabric_label'] == series['Storage_Fabric_label'])
        zoning_valid_df = storage_host_zone_df.loc[mask_same_fabic].copy()
        # if zones are found return string of zones separated by commas
        if not zoning_valid_df.empty:
            zone_sr = zoning_valid_df['zone'].drop_duplicates()
            zones_str = ', '.join(zone_sr.to_list())
            return zones_str


def storage_host_report(storage_host_aggregated_df, data_names, report_headers_df, report_columns_usage_sr):
    """Function to create storage_host and storage_host fabric_label comparision DataFrames"""

    if storage_host_aggregated_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    storage_host_report_df = storage_host_aggregated_df.copy()
    # dataframe where hosts and storage port are in the same fabric or host imported to storage fabric
    mask_local_imported = storage_host_aggregated_df['Fabric_host_status'].isin(['local', 'remote_imported'])
    storage_host_valid_df = storage_host_aggregated_df.loc[mask_local_imported].copy()

    # drop uninformative columns 
    storage_host_report_df = clean_storage_host(storage_host_report_df)
    storage_host_valid_df = clean_storage_host(storage_host_valid_df)
    
    storage_host_valid_df = dfop.remove_duplicates_from_column(storage_host_valid_df, 'System_Name',
                                                                duplicates_subset=['configname', 'System_Name'])   
    # slice required columns and translate header
    storage_host_report_df = dfop.generate_report_dataframe(storage_host_report_df, report_headers_df, report_columns_usage_sr, data_names[1])
    dfop.drop_slot_value(storage_host_report_df, report_columns_usage_sr)
    storage_host_valid_df = dfop.generate_report_dataframe(storage_host_valid_df, report_headers_df, report_columns_usage_sr, data_names[1])
    dfop.drop_slot_value(storage_host_valid_df, report_columns_usage_sr)
    # translate values in columns
    storage_host_report_df = dfop.translate_values(storage_host_report_df)
    storage_host_valid_df = dfop.translate_values(storage_host_valid_df)
    # create comparision storage_host DataFrame based on Fabric_labels
    slice_column = 'Подсеть' if 'Подсеть' in storage_host_valid_df.columns else 'Подсеть порта массива'
    storage_host_compare_report_df = dfop.dataframe_slice_concatenate(storage_host_valid_df, column=slice_column)
    return storage_host_report_df, storage_host_compare_report_df


def clean_storage_host(df):
    """Function to clean storage_host and storage_host_valid (storage port and host are in the same fabric)"""

    # drop second column in each tuple of the list if values in columns of the tuple are equal
    df = dfop.drop_equal_columns(df, columns_pairs=[('Host_Wwnp', 'Host_Wwn'), 
                                                ('Device_Host_Name_per_fabric_name_and_label', 'Device_Host_Name_per_fabric_label'),
                                                ('Device_Host_Name_total_fabrics', 'Device_Host_Name_per_fabric_name')])
    # drop empty columns
    df = dfop.drop_column_if_all_na(df, ['Device_Port', 'Device_Location'])
    # drop columns where all values are equal to the item value
    columns_values = {'Host_Storage_Fabric_equal': 'Yes', 'Persona_correct': 'Yes', 'Fabric_host_status': 'local'}
    df = dfop.drop_all_identical(df, columns_values, dropna=True)
    # drop second pair of Fabric_name, Fabric_label if the columns are respectively equal 
    df = dfop.drop_equal_columns_pairs(df, columns_main=['Storage_Fabric_name', 'Storage_Fabric_label'], 
                                        columns_droped=['Host_Fabric_name', 'Host_Fabric_label'], dropna=False)
    # rename first pair of Fabric_name, Fabric_label if second one was droped in prev step
    if not 'Host_Fabric_name' in df.columns:
        df.rename(columns={'Storage_Fabric_name': 'Fabric_name', 'Storage_Fabric_label': 'Fabric_label'}, inplace=True)
    return df


        


