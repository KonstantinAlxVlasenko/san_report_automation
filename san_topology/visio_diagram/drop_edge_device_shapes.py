
"""Module drops edge devices on Visio page"""

import re

from tqdm import tqdm

import utilities.database_operations as dbop
from san_automation_constants import SERVER_DESC, STORAGE_DESC

from .drop_connector_shape import drop_connector_shape, shape_font_change
from .visio_document import activate_visio_page, get_tqdm_desc_indented


def add_visio_device_shapes(san_links_df, visio, stn, fabric_label_colours, 
                            visio_log_file, san_topology_constantants_sr, 
                            tqdm_max_desc_len, tqdm_ncols_num, tqdm_desc_str):
    """Function to add servers, storages and libraries shapes to Visio document page and
    create connections between device shapes and switch shapes"""    

    
    fabric_name_prev = None
    x_coordinate = 0
    x_start = int(san_topology_constantants_sr['x_start'])
    link_font_size = san_topology_constantants_sr['link_font_size']
    
    if tqdm_desc_str == SERVER_DESC:
        device_font_size = san_topology_constantants_sr['server_font_size']
    elif tqdm_desc_str == STORAGE_DESC:
        device_font_size = san_topology_constantants_sr['storage_font_size']

    dbop.add_log_entry(visio_log_file, '\nEdge devices')
    tqdm_desc_indented = get_tqdm_desc_indented(tqdm_desc_str, tqdm_max_desc_len)
    
    
    # find unique devices on fabric_name -> device_shapename -> device_class level
    san_device_shapes_df = find_device_shapes(san_links_df)
    
    for _, (_, device_sr) in zip(
        tqdm(range(len(san_device_shapes_df.index)), desc=tqdm_desc_indented, ncols=tqdm_ncols_num), 
        san_device_shapes_df.iterrows()):
        
        dbop.add_log_entry(visio_log_file, '\n', '-'*30, device_sr.to_string())
        fabric_name_current = device_sr['Fabric_name']
        # activate page with shape to be dropped 
        page = activate_visio_page(visio, page_name=fabric_name_current)
        
        # reset x-coordinate if new page (fabric_name) is activated
        if fabric_name_current != fabric_name_prev:
            x_coordinate = x_start
            fabric_name_prev = fabric_name_current
        
        # print(fabric_name_current, device_sr['Device_shapeText'])   
        # drop shape on active page
        drop_device_shape(device_sr, page, stn, x_coordinate, device_font_size)
        # filter device_shapename links for the dropped shape
        # each row is switch -> device_shapename link with link quantity
        # device_shapename is single device or list of devices groped on fabric connection description
        shape_links_df = find_device_shape_links(device_sr, san_links_df)
        dbop.add_log_entry(visio_log_file, shape_links_df.to_string())
        
        # print(shape_links_df)
        for _, link_sr in shape_links_df.iterrows():
            # drop device_shapename -> switch links
            drop_connector_shape(link_sr, page, stn, fabric_label_colours, link_font_size, 
                                    source='Device_shapeName', destination='switch_shapeName')    
        # shift x-coordinate to the right for the next device_shapename location
        x_coordinate =  x_coordinate + device_sr['x_group_offset']



def find_device_shapes(san_links_df):
    """Function to filter unique devices or device groups on 
    fabric_name -> device_shapename -> device_class level"""

    san_device_shapes_df = san_links_df.drop_duplicates(subset=['Fabric_name', 'Device_shapeName', 'deviceType']).copy()
    san_device_shapes_df = san_device_shapes_df[
        ['Fabric_name', 'Device_shapeText', 'Device_shapeName',  'deviceType', 'master_shape', 'y_graph_level', 'x_group_offset']]
    return san_device_shapes_df



def drop_device_shape(device_sr, page, stn, x_coordinate, device_font_size):
    """Function to drop shape on page with x_coordinate"""
    
    master = stn.Masters(device_sr['master_shape'])
    # default_font_size = '12 pt'

    # drop page on page with x, y coordinates
    device_shape = page.Drop(master, x_coordinate, device_sr['y_graph_level'])
    device_shape.Name = device_sr['Device_shapeName']
    
    shape_text = device_sr['Device_shapeText']
    # synergy and blade server names are each on new line
    # replace ': ' and ', ' in device_shapetext with new line
    if device_sr['deviceType'] in ['SRV_BLADE', 'SRV_SYNERGY']:
        shape_text = re.sub(': |, ', '\n', shape_text)
        
    device_shape.Text = shape_text
    
    shape_font_change(device_shape, device_font_size)
    
    # if device_font_size != default_font_size:
    #     device_shape.Cells("Char.Size").FormulaU = device_font_size
    
    # # make title bold
    # shape_chars = device_shape.Characters
    # device_shape.CharProps(2, 1)
    return device_shape


def find_device_shape_links(device_sr, san_links_df):
    """Function to filter device_shapename links in the fabric_name. 
    Each row is switch -> device_shapename link with link quantity for the device_sr fabric_name.
    Device_shapename is a single device or list of devices groped on fabric connection description"""

    mask_fabric_name = san_links_df['Fabric_name'] == device_sr['Fabric_name']
    mask_shape_name = san_links_df['Device_shapeName'] == device_sr['Device_shapeName']
    mask_device_type = san_links_df['deviceType'] == device_sr['deviceType']
    return san_links_df.loc[mask_fabric_name & mask_shape_name & mask_device_type]