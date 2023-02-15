
"""Module drops switch and VC module pairs on Visio page"""

import math
from itertools import product

from tqdm import tqdm

import utilities.database_operations as dbop

from .drop_connector_shape import drop_connector_shape, shape_font_change
from .visio_document import activate_visio_page, get_tqdm_desc_indented


# SWITCH_CLASS_PRODUCT = [', '.join(switch_class) for switch_class in product(['ENTRY', 'MID', 'ENTP'], repeat=2)]

def add_visio_switch_shapes(san_graph_sw_pair_df, visio, stn, visio_log_file, san_topology_constantants_sr, tqdm_max_desc_len, tqdm_ncols_num, tqdm_desc_str):
    """Function to add swith and VC shapes to Visio document pages (fabric_name)""" 

    fabric_name_prev = None
    graph_level_prev = None
    x_group_current = 0
    x_start = int(san_topology_constantants_sr['x_start'])
    switch_font_size = san_topology_constantants_sr['switch_font_size']

    dbop.add_log_entry(visio_log_file, '\nSwitches\n', 'Fabric, switchName, switchWwn, swithNumber_in_pair, x-coordinate, y-coordinate')
        
    tqdm_desc_indented = get_tqdm_desc_indented(tqdm_desc_str, tqdm_max_desc_len)

    for _, (_, switch_pair_sr) in zip(
        tqdm(range(len(san_graph_sw_pair_df.index)), desc=tqdm_desc_indented, ncols=tqdm_ncols_num), 
        san_graph_sw_pair_df.iterrows()):
    
        fabric_name_current = switch_pair_sr['Fabric_name']
        # graph_level: core, edge, ag
        graph_level_current = switch_pair_sr['graph_level']
        # activate page with switch_pair
        page = activate_visio_page(visio, page_name=fabric_name_current)
        # reset x-coordinate if new page (fabric_name) selected
        if fabric_name_current != fabric_name_prev:
            x_group_current = x_start
            fabric_name_prev = fabric_name_current            
        # reset x-coordinate if shape drop level (y_graph_level) changed   
        if graph_level_current != graph_level_prev:
            x_group_current = x_start
            graph_level_prev = graph_level_current
        
        drop_switch_pair_shapes(switch_pair_sr, x_group_current, page, stn, visio_log_file, switch_font_size)
        # move cursor to the next switch pair location x-coordinate
        x_group_current = x_group_current + switch_pair_sr['x_group_offset']
        


def drop_switch_pair_shapes(switch_pair_sr, x_group_current, page, stn, visio_log_file, switch_font_size):
    """Function to drop switch shapes from single switch_pair to the active visio page and
    add shape text (to all shapes of the switch_pair in case of directors and to 
    the bottom shape onlyk in case if switches and vcs"""

    
    # split switch_pair_srs details to build Visio graph
    master_shapes = switch_pair_sr['master_shape'].split(', ')
    shape_names = switch_pair_sr['switchName_Wwn'].split(', ')
    shape_text = switch_pair_sr['switchName_DID'].split('/ ')[::-1]

    sw_quantity = len(switch_pair_sr['switchClass_mode'].split(', '))
    SWITCH_CLASS_PRODUCT = [', '.join(switch_class) for switch_class in product(['ENTRY', 'MID', 'ENTP'], repeat=sw_quantity)]
    
    # first drop second switch of the switch_pair_sr to make the first switch visually overlap to the second switch 
    # two ENT swtiches sw1 and sw2 -> [(0, 'ENT', 'sw2'), (1, 'ENT', 'sw1')]
    for i, master_shape, shape_name in list(zip(range(len(master_shapes)-1, -1, -1), master_shapes, shape_names))[::-1]:
        
        # choose shape icon
        master = stn.Masters(master_shape)
        # set x, y coordinates
        # x_shape_offset is for Direcors only (for all other devices it's 0)
        x = x_group_current - i * switch_pair_sr['x_shape_offset']
        # y_shape_offset makes fabic_label A switch shape to be located above fabic_label B switch shape
        y = switch_pair_sr['y_group_level'] + i * switch_pair_sr['y_shape_offset']
                
        # add enrty to visio graph creation log
        log_entry = ' '.join([switch_pair_sr['Fabric_name'], shape_name, str(i), str(x), str(y)])
        dbop.add_log_entry(visio_log_file, log_entry)
        
        # add switch shape to the Visio page
        shape = page.Drop(master, x, y)
        shape.Name = shape_name
        
        # for Directors shape text is under each shape with
        # Director name, DID and model individually
        if 'DIR' in switch_pair_sr['switchClass_mode']:
            shape.Text = shape_text[i] + "\n" + switch_pair_sr['ModelName']
            shape_font_change(shape, switch_font_size)
        else:
            shape.Text = " "
    
    # for all switches except Directors shape text with switch name, DID and model 
    # for the switch pair on the bottom shape of the switch pair
    if not 'DIR' in switch_pair_sr['switchClass_mode']:
        bottom_shape = page.Shapes.ItemU(shape_names[-1])
        if switch_pair_sr['ModelName']:
            bottom_shape.Text = switch_pair_sr['switchName_DID'] + "\n" + switch_pair_sr['ModelName']
        else:
            bottom_shape.Text = switch_pair_sr['switchName_DID']
        shape_font_change(bottom_shape, switch_font_size)
        if switch_pair_sr['switchClass_mode'] in SWITCH_CLASS_PRODUCT:
            bottom_shape.Cells("TxtWidth").FormulaU = get_textbox_width(switch_pair_sr['switchName_DID'])
        
def get_textbox_width(text):

    width_ratio = math.ceil(2.6*len(text)*10/74)/10 + 0.1
    return f"Width * {width_ratio if width_ratio >= 1.5 else 1.5}"


def add_visio_inter_switch_connections(inter_switch_links_df, visio, stn, fabric_label_colours_dct, 
                                        visio_log_file, san_topology_constantants_sr,
                                        tqdm_max_desc_len, tqdm_ncols_num, tqdm_desc_str):
    """Function to create inter switch shape connections"""
    
    if inter_switch_links_df.empty:
        return None
        
    link_font_size = san_topology_constantants_sr['link_font_size']

    # log entry header
    dbop.add_log_entry(visio_log_file, '\nLinks\n', 'Fabric, switchName, switchWwn ----> Connected_switchName, Connected_switchWwn')
    tqdm_desc_indented = get_tqdm_desc_indented(tqdm_desc_str, tqdm_max_desc_len)
    
    for _, (_, link_sr) in zip(
        tqdm(range(len(inter_switch_links_df.index)), desc=tqdm_desc_indented, ncols=tqdm_ncols_num), 
        inter_switch_links_df.iterrows()):

        # add connection log entry
        log_entry =  ' '.join([link_sr['Fabric_name'], link_sr['shapeName'], '  ---->  ', link_sr['Connected_shapeName']])
        dbop.add_log_entry(visio_log_file, log_entry)
        
        # activate page with shapes to be connected 
        fabric_name_current = link_sr['Fabric_name']
        page = activate_visio_page(visio, page_name=fabric_name_current)
        # drop connection between shapes
        drop_connector_shape(link_sr, page, stn, fabric_label_colours_dct, link_font_size, 
                                source='shapeName', destination='Connected_shapeName')


def group_switch_pairs(san_graph_sw_pair_df, visio, visio_log_file, 
                        tqdm_max_desc_len, tqdm_ncols_num, tqdm_desc_str):
    """Function to create Visio groups for each switch pair"""
    
    dbop.add_log_entry(visio_log_file, '\nSwitch groups')
    tqdm_desc_indented = get_tqdm_desc_indented(tqdm_desc_str, tqdm_max_desc_len)
    
    
    
    for _, (_, switch_pair_sr) in zip(
        tqdm(range(len(san_graph_sw_pair_df.index)), desc=tqdm_desc_indented, ncols=tqdm_ncols_num), 
        san_graph_sw_pair_df.iterrows()):
        
        dbop.add_log_entry(visio_log_file, '\n', switch_pair_sr.to_string())
        fabric_name_current = switch_pair_sr['Fabric_name']
        # activate page with shapes to be grouped 
        page = activate_visio_page(visio, page_name=fabric_name_current)
        active_window = visio.ActiveWindow
        shape_names = switch_pair_sr['switchName_Wwn'].split(', ')
        
        if len(shape_names) > 1:
            # drop any previous selections if they exist
            active_window.DeselectAll()
            # select all switches from switch_pair
            for shape_name in shape_names:
                # print(shape_name)
                active_window.Select(page.Shapes.ItemU(shape_name), 2)
            # group all switches from switch_pair
            active_window.Selection.Group()
            # set name attribute for the group
            sw_group = page.Shapes.ItemU(len(page.Shapes))
            sw_group.Name = ' - '.join(shape_names)