import os
import re
from datetime import date
from itertools import chain

import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
from san_automation_constants import (HPE_PALETTE, ISL_DESC, LEFT_INDENT,
                                      MIDDLE_SPACE, NPIV_DESC, RIGHT_INDENT,
                                      SERVER_DESC, STORAGE_DESC, SWITCH_DESC,
                                      SWITCH_GROUPS_DESC)

from .edge_device_shapes import add_visio_device_shapes
from .switch_shapes import (add_visio_inter_switch_connections,
                            add_visio_switch_shapes, group_switch_pairs)
from .visio_document import save_visio_document, visio_document_init


def visio_diagram_init(san_graph_switch_df, san_graph_sw_pair_df, 
                        san_graph_isl_df, san_graph_npiv_df, 
                        storage_shape_links_df, server_shape_links_df, 
                        san_graph_sw_pair_group_df, 
                        fabric_name_duplicated_sr, fabric_name_dev_sr, 
                        project_constants_lst, 
                        software_path_sr, san_topology_constantants_sr):
    """Function to initialize SAN topology drawing in Visio"""
    
    # imported project constants required for module execution
    # project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst
    project_steps_df, max_title, io_data_names_df, report_requisites_sr, *_ = project_constants_lst


    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'visio_diagram')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    

    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)

    if force_run:

        first_run = True if data_lst[0] is None else False
        # aggregated DataFrames
        visio = visio_diagram_aggregated(san_graph_switch_df, san_graph_sw_pair_df, 
                                                    san_graph_isl_df, san_graph_npiv_df, 
                                                    storage_shape_links_df, server_shape_links_df, 
                                                    san_graph_sw_pair_group_df, 
                                                    fabric_name_duplicated_sr, fabric_name_dev_sr, 
                                                    project_constants_lst, software_path_sr, san_topology_constantants_sr, first_run)
        
        # current operation information string
        info = f'Creating Visio Diagram'
        print(info, end =" ") 
        # after finish display status
        status = 'ok' if visio else 'skip'
        meop.status_info(status, max_title, len(info))
        
        visio_diagram_sr = save_visio_document(visio, report_requisites_sr)

        if visio:

            # current operation information string
            info = f'Saving Visio file'
            print(info, end =" ") 
            # after finish display status
            meop.status_info('ok', max_title, len(info))
        
        # create list with partitioned DataFrames
        data_lst = [visio_diagram_sr]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst) 
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        visio_diagram_sr, *_ = data_lst
    
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return visio_diagram_sr


def visio_diagram_aggregated(san_graph_switch_df, san_graph_sw_pair_df, 
                            san_graph_isl_df, san_graph_npiv_df, 
                            storage_shape_links_df, server_shape_links_df, 
                            san_graph_sw_pair_group_df, 
                            fabric_name_duplicated_sr, fabric_name_dev_sr, 
                            project_constants_lst, software_path_sr, san_topology_constantants_sr, first_run):
    
    
    if first_run:
        print('\n')
        query = "Do you want to create Visio diagram? (y)es/(n)o: "
        reply = meop.reply_request(query)
        if reply == 'n':
            return None
        print('\n')    
    
    # imported project constants required for module execution
    # project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst
    _, max_title, _, report_requisites_sr, *_ = project_constants_lst


    fabric_name_duplicated_lst = fabric_name_duplicated_sr.tolist()
    fabric_name_dev_lst = fabric_name_dev_sr.tolist()

    visio_log_file = get_log_file_path(report_requisites_sr)

    start_time = f'start: {meop.current_datetime()}'
    dbop.add_log_entry(visio_log_file, '*'*40, start_time)


    fabric_name_lst = list(san_graph_sw_pair_df['Fabric_name'].unique())
    # fabric_labels = sorted(list(switch_pair_df['Fabric_label'].unique()))
    fabric_labels = sorted(list(san_graph_switch_df['Fabric_label'].unique()))

    
    hpe_pallete_lst = [HPE_PALETTE[colour] for colour in ('green', 'red', 'blue', 'purple')]
    
    fabric_label_colours_dct = dict(zip(fabric_labels, hpe_pallete_lst))


    
    tqdm_max_desc_len = get_tqdm_max_desc_len(san_graph_isl_df, san_graph_npiv_df)
    tqdm_ncols_num = get_tqdm_ncols_num(max_title)
    
    visio_template_path = software_path_sr['viso_template_path']
    visio_stencil_path = software_path_sr['viso_stencil_path']

    
    # initialize Visio Documet with template
    visio, stn = visio_document_init(visio_template_path, visio_stencil_path, fabric_name_lst, report_requisites_sr)
    # current operation information string
    info = f'Creating Visio document template'
    print(info, end =" ")
    # after finish display status
    meop.status_info('ok', max_title, len(info))


    # add swith and vc shapes
    add_visio_switch_shapes(san_graph_sw_pair_df, visio, stn, 
                            visio_log_file, san_topology_constantants_sr, 
                            tqdm_max_desc_len, tqdm_ncols_num, tqdm_desc_str=SWITCH_DESC)
    
    
    # add isl links
    add_visio_inter_switch_connections(san_graph_isl_df, visio, stn, fabric_label_colours_dct,
                                        visio_log_file, san_topology_constantants_sr, 
                                        tqdm_max_desc_len, tqdm_ncols_num, tqdm_desc_str=ISL_DESC)
    # add npiv links
    add_visio_inter_switch_connections(san_graph_npiv_df, visio, stn, fabric_label_colours_dct,
                                        visio_log_file, san_topology_constantants_sr, 
                                        tqdm_max_desc_len, tqdm_ncols_num, tqdm_desc_str=NPIV_DESC)

    # add pages for duplicated fabric_names
    for fabric_name_duplicated, fabric_name_device in zip(fabric_name_duplicated_lst, fabric_name_dev_lst):
        visio.ActiveWindow.Page = fabric_name_duplicated
        visio.ActivePage.Duplicate()
        visio.ActivePage.Name = fabric_name_device


    # add server and unknown
    add_visio_device_shapes(server_shape_links_df, visio, stn, fabric_label_colours_dct, 
                            visio_log_file, san_topology_constantants_sr,
                            tqdm_max_desc_len, tqdm_ncols_num, tqdm_desc_str=SERVER_DESC)
    # add storage and lib
    add_visio_device_shapes(storage_shape_links_df, visio, stn, fabric_label_colours_dct,
                            visio_log_file, san_topology_constantants_sr,
                            tqdm_max_desc_len, tqdm_ncols_num, tqdm_desc_str=STORAGE_DESC) #shape_font_key='storage_font_size')
    # create visio groups for switch Pairs
    group_switch_pairs(san_graph_sw_pair_group_df, visio, visio_log_file, 
                        tqdm_max_desc_len, tqdm_ncols_num, tqdm_desc_str=SWITCH_GROUPS_DESC)

    finish_time = f'finish: {meop.current_datetime()}'
    dbop.add_log_entry(visio_log_file,  '\n', finish_time, '^'*40, )
    
    return visio


def get_log_file_path(report_requisites_sr, current_date=str(date.today())):

    file_name = report_requisites_sr['customer_name'] + '_Visio_log_' + current_date + '.log'
    file_path = os.path.join(report_requisites_sr['today_report_folder'], file_name)
    return file_path


def get_tqdm_max_desc_len(san_graph_isl_df, san_graph_npiv_df):
    
    desc_link_lst = [desc for (desc, link_df) in zip(
        (ISL_DESC, NPIV_DESC), 
        (san_graph_isl_df, san_graph_npiv_df)
        ) if not link_df.empty]
    desc_lst = [SWITCH_DESC, STORAGE_DESC, SERVER_DESC, SWITCH_GROUPS_DESC]
    desc_lst.extend(desc_link_lst)
    return max([len(desc) for desc in desc_lst])


def get_tqdm_ncols_num(max_title):
    
    return max_title + MIDDLE_SPACE - RIGHT_INDENT      
