"""Module to add port cfg and sfp related information to aggregated portcmd DataFrame.
Find ports for which error threshold is exceeded."""


import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.report_operations as report
import utilities.servicefile_operations as sfop

from .port_err_cfg import port_cfg_join, port_error_filter
from .port_sfp import port_sfp_join
from .port_sfp_statistics import count_sfp_statistics


def port_err_sfp_cfg_analysis(portshow_aggregated_df, sfpshow_df, portcfgshow_df,
                                project_constants_lst):
    """Main function to add porterr, transceiver and portcfg information to portshow DataFrame"""
    
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, _, report_headers_df, report_columns_usage_sr, *_ = project_constants_lst
    
    exit_after_save_flag = False
    portshow_sfp_force_flag = False
    portshow_sfp_export_flag = project_steps_df.loc['portshow_sfp_aggregated', 'export_to_excel']

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'port_err_sfp_cfg_analysis_out', 'port_err_sfp_cfg_analysis_in')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)

    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data    
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title, analyzed_data_names)

    if force_run:
        # import transeivers information from file
        sfp_model_df = sfop.dataframe_import('sfp_models', max_title)
        # data imported from init file (regular expression patterns) to extract values from data columns
        pattern_dct, *_ = sfop.regex_pattern_import('common_regex', max_title)        
        # current operation information string
        info = 'Updating connected devices table with port cfg and sfp'
        print(info, end =" ") 
        
        # add portcfg information
        portshow_sfp_aggregated_df = port_cfg_join(portshow_aggregated_df, portcfgshow_df)
        # add sfp readings, sfp model details, sfp redings intervals
        portshow_sfp_aggregated_df = port_sfp_join(portshow_sfp_aggregated_df, sfpshow_df, sfp_model_df, pattern_dct)
        # after finish display status
        meop.status_info('ok', max_title, len(info))

        # current operation information string
        info = 'Counting sfp statistics'
        print(info, end =" ") 
        sfp_statistics_df = count_sfp_statistics(portshow_sfp_aggregated_df, pattern_dct)
        meop.status_info('ok', max_title, len(info))

        # current operation information string
        info = 'Filtering ports with exceeding error thresholds'
        print(info, end =" ")
        # find ports which exceed error threshold 
        filtered_error_lst = port_error_filter(portshow_sfp_aggregated_df)
        # after finish display status
        meop.status_info('ok', max_title, len(info))

        # warning if UKNOWN SFP present
        if (portshow_sfp_aggregated_df['Transceiver_Supported'] == 'Unknown SFP').any():
            info_columns = ['Fabric_name', 'Fabric_label', 
                            'configname', 'chassis_name', 'chassis_wwn', 
                            'slot',	'port', 'Transceiver_Supported']
            portshow_sfp_info_df = portshow_sfp_aggregated_df.drop_duplicates(subset=info_columns).copy()
            unknown_count = len(portshow_sfp_info_df[portshow_sfp_info_df['Transceiver_Supported'] == 'Unknown SFP'])
            info = f'{unknown_count} {"port" if unknown_count == 1 else "ports"} with UNKNOWN supported SFP tag found'
            print(info, end =" ")
            meop.status_info('warning', max_title, len(info))
            # ask if save portshow_aggregated_df
            if not portshow_sfp_export_flag:
                reply = meop.reply_request("Do you want to save 'portshow_sfp_aggregated'? (y)es/(n)o: ")
                if reply == 'y':
                    portshow_sfp_force_flag = True
            exit_after_save_flag = meop.display_stop_request(exit_after_save_flag)
        # create report tables from port_complete_df DataFrtame
        port_err_sfp_cfg_report_lst = port_err_sfp_cfg_report(
            portshow_sfp_aggregated_df, data_names[2:5], report_headers_df, report_columns_usage_sr)
        sfp_statistics_report_df = sfp_statistics_report(sfp_statistics_df, report_headers_df, report_columns_usage_sr)
        
        data_lst = [portshow_sfp_aggregated_df, sfp_statistics_df, *port_err_sfp_cfg_report_lst, 
                    sfp_statistics_report_df, *filtered_error_lst]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty and reset DataFrame if yes
    else:    
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        portshow_sfp_aggregated_df, *_ = data_lst
    
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        force_flag = False
        if data_name == 'portshow_sfp_aggregated':
            force_flag = portshow_sfp_force_flag
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst, force_flag=force_flag)
    # check if stop programm execution flag is on
    meop.validate_stop_program_flag(exit_after_save_flag)
    return portshow_sfp_aggregated_df


def port_err_sfp_cfg_report(port_complete_df, data_names, report_headers_df, report_columns_usage_sr):
    """Function to create required report DataFrames out of aggregated DataFrame"""

    # add speed value column for fillword verification in errors_report_df
    port_complete_df['speed_fillword'] = port_complete_df['speed']
    errors_report_df, sfp_report_df, portcfg_report_df = \
        report.generate_report_dataframe(port_complete_df, report_headers_df, report_columns_usage_sr, *data_names)
    port_err_sfp_cfg_report_lst = [errors_report_df, sfp_report_df, portcfg_report_df]
    # drop empty columns
    for report_df in port_err_sfp_cfg_report_lst:
        report_df.dropna(axis=1, how = 'all', inplace=True)
    return port_err_sfp_cfg_report_lst


def sfp_statistics_report(sfp_statistics_df, report_headers_df, report_columns_usage_sr):

    # npiv statistics report
    sfp_statistics_report_df = report.statistics_report(sfp_statistics_df, report_headers_df, 'Статистика_SFP_перевод', 
                                                    report_columns_usage_sr, drop_columns=['chassis_wwn', 'switchWwn'])
    # remove zeroes to clean view
    dfop.drop_zero(sfp_statistics_report_df)
    sfp_statistics_report_df.dropna(axis=1, how = 'all', inplace=True)
    return sfp_statistics_report_df