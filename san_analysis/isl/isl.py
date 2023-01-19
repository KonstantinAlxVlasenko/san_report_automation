"""Module to generate 'InterSwitch links', 'InterFabric links' customer report tables"""


import pandas as pd
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .isl_aggregation import isl_aggregated
from .isl_statistics import isl_statistics


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
        # verify missing switches
        mask_switch_missing = isl_aggregated_df['Connected_SwitchName'].isna()
        missing_switch_df =  isl_aggregated_df.loc[mask_switch_missing].drop_duplicates(subset=['Connected_switchWwn']).copy()

        if missing_switch_df.empty:    
            meop.status_info('ok', max_title, len(info))
        else:
            meop.status_info('warning', max_title, len(info))
            missing_sw_num = missing_switch_df['Connected_switchWwn'].count()
            print(f'\nWARNING!!! {missing_sw_num} switch {"config is" if missing_sw_num == 1 else "configs are"} missing!\n')

        # create list with partitioned DataFrames
        data_lst = [isl_aggregated_df, fcredge_aggregated_df]

        # data_lst = [isl_aggregated_df, isl_statistics_df, isl_report_df, ifl_report_df, isl_statistics_report_df]

        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        isl_aggregated_df, fcredge_aggregated_df, *_ = data_lst
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return isl_aggregated_df, fcredge_aggregated_df
