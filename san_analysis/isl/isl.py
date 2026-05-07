"""Module to generate 'InterSwitch links', 'InterFabric links' customer report tables"""


import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.report_operations as report


from .isl_aggregation import isl_aggregated


def isl_analysis(fabricshow_ag_labels_df, switch_params_aggregated_df,  
            isl_df, trunk_df, lsdb_df, fcredge_df, portshow_df, sfpshow_df, 
            portcfgshow_df, switchshow_ports_df, project_constants_lst):
    """Main function to create ISL and IFR report tables"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'isl_analysis_out', 'isl_analysis_in')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)

    # flag to forcible save isl_aggregated_df if required
    isl_force_flag = False
    exit_after_save_flag = False 
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, 
                                            max_title, analyzed_data_names)
    if force_run:
        # data imported from init file (regular expression patterns) to extract values from data columns
        pattern_dct, _ = sfop.regex_pattern_import('common_regex', max_title)

        # current operation information string
        info = f'Generating ISL and IFL tables'
        print(info, end =" ")

        # get aggregated DataFrames
        isl_aggregated_df, fcredge_aggregated_df = \
            isl_aggregated(fabricshow_ag_labels_df, switch_params_aggregated_df, 
            isl_df, trunk_df, lsdb_df, fcredge_df, portshow_df, sfpshow_df, portcfgshow_df, switchshow_ports_df, pattern_dct)
        
        # after finish display status
        meop.status_info('ok', max_title, len(info))
        # show WARNING notification if switch wo config discovered or switch wwn 00:00:00:00:00:00:00:00 found
        isl_force_flag, exit_after_save_flag = warning_notification(isl_aggregated_df, project_steps_df, max_title)       
        # create list with partitioned DataFrames
        data_lst = [isl_aggregated_df, fcredge_aggregated_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        isl_aggregated_df, fcredge_aggregated_df, *_ = data_lst
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        force_flag = False
        if data_name == 'isl_aggregated':
            force_flag = isl_force_flag
        if data_name != 'report_columns_usage_upd':
            report.dataframe_to_excel(data_frame, data_name, project_constants_lst, force_flag=force_flag)
    # check if stop programm execution flag is on
    meop.validate_stop_program_flag(exit_after_save_flag)
    return isl_aggregated_df, fcredge_aggregated_df


def warning_notification(isl_aggregated_df, project_steps_df, max_title):
    """Function to show WARNING notification if switch wo config discovered in isl or trunk
    or switch wwn 00:00:00:00:00:00:00:00 found in isl or trunk"""


    isl_force_flag = False
    exit_after_save_flag = False

    isl_export_flag = project_steps_df.loc['isl_aggregated', 'export_to_excel']

    # verify missing switches
    mask_switch_missing = isl_aggregated_df['Connected_SwitchName'].isna()
    missing_switch_df =  isl_aggregated_df.loc[mask_switch_missing].drop_duplicates(subset=['Connected_switchWwn']).copy()
    if not missing_switch_df.empty:
        missing_sw_num = missing_switch_df['Connected_switchWwn'].count()
        info = f'Missing switch config(s) detected: {missing_sw_num} item(s)'
        print(info, end =" ")
        meop.status_info('warning', max_title, len(info))

    # verify switch wwn 00:00:00:00:00:00:00:00
    mask_switch_wwn_zero = isl_aggregated_df['Connected_switchWwn'] == '00:00:00:00:00:00:00:00'
    switch_wwn_zero_df = isl_aggregated_df.loc[mask_switch_wwn_zero].copy()
    if not switch_wwn_zero_df.empty:
        sw_wwn_zero_num = switch_wwn_zero_df['Connected_switchWwn'].count()
        info = f'Connected switch wwn "00:00:00:00:00:00:00:00" for {sw_wwn_zero_num} isl(s) detected'
        print(info, end =" ")
        meop.status_info('warning', max_title, len(info))

    if not missing_switch_df.empty or not switch_wwn_zero_df.empty:
        # ask if save isl_aggregated_df
        if not isl_export_flag:
            reply = meop.reply_request("\nDo you want to save 'isl_aggregated'? (y)es/(n)o: ")
            print('\n')
            if reply == 'y':
                isl_force_flag = True
        exit_after_save_flag = meop.display_stop_request(exit_after_save_flag)
    return isl_force_flag, exit_after_save_flag