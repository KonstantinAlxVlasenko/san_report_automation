# -*- coding: utf-8 -*-
"""
Created on Thu May 12 13:51:11 2022

@author: vlasenko
"""

import win32com.client
import os
import pandas as pd
import re
import time
from datetime import datetime
import general_cmd_module as dfop

script_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)

# DataLine OST
db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN OST\NOV2022\database_DataLine OST"
db_file = r"DataLine OST_analysis_database.db"


data_names = ['san_graph_sw_pair', 'san_graph_isl', 'san_graph_npiv', 'storage_shape_links', 'server_shape_links', 'san_graph_sw_pair_group', 'fabric_name_duplicated', 'fabric_name_dev']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)



switch_params_aggregated_df, isl_aggregated_df, switch_pair_df, isl_statistics_df, npiv_statistics_df, *_ = data_lst




log_file = "visio_graph_log.txt"

san_template_path = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts\SAN_Drawings_template_DataLine3.vsdm'
stencil_path = r"C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts\san_assessment_stn_upd.vssx"


HPE_PALETTE = ['RGB(0,169,103)', 'RGB(255,141,109)']
LINE_PATTERN = {2: 'USE("Parallel Lines x2")', 3: 'USE("Parallel Lines x3")', 4: 'USE("Parallel Lines x4")', '4+': 'USE("Parallel Lines x4+")'}
X_START = 35
SHAPE_FONT_SIZE = "12 pt"
LINK_FONT_SIZE = "12 pt"

# str_dev = connected_devices_df.to_string()




start_time = f'start: {current_datetime()}'
add_log_entry(log_file, '*'*40, start_time)

fabric_name_lst = list(san_graph_sw_pair_df['Fabric_name'].unique())
fabric_labels = sorted(list(switch_pair_df['Fabric_label'].unique()))
fabric_label_colours_dct = dict(zip(fabric_labels, HPE_PALETTE))




# doc = visio.ActiveDocument
# # page = visio.ActiveWindow
# # for shape in page.Shapes:
# #     print(shape.Text)


# for page in doc.Pages:
#     for shape in page.Shapes:

#         print(shape.Text)

#         if shape.Text == 'Customer / SAN Assessment':
#             shape.Text = 'Data Line' + ' / ' + 'SAN Inventory'


# for page in doc.Pages:
#     for shape in page.Shapes:
#         if shape.Text == 'Fabric Name':
#             shape.Text = 'Фабрика ' + page.Name
#         elif project_name and shape.Text == 'Project title':
#             shape.Text = project_name




# HPE_PALETTE = {'green': 'RGB(0,169,103)',  'red': 'RGB(255,141,109)'}

# HPE_PALETTE.values() = {'green': 'RGB(0,169,103)',  'red': 'RGB(255,141,109)'}


# initialize Visio Documet with template
visio, stn = visio_document_init(san_template_path, stencil_path, fabric_name_lst)
# add swith and vc shapes
add_visio_switch_shapes(san_graph_sw_pair_df, visio, stn)
# add isl links
add_visio_inter_switch_connections(san_graph_isl_df, visio, stn, fabric_label_colours_dct)
# add npiv links
add_visio_inter_switch_connections(san_graph_npiv_df, visio, stn, fabric_label_colours_dct)


####################
# add pages for duplicated fabric_names
for fabric_name_duplicated, fabric_name_device in zip(fabric_name_duplicated_lst, fabric_name_dev_lst):
    visio.ActiveWindow.Page = fabric_name_duplicated
    visio.ActivePage.Duplicate()
    visio.ActivePage.Name = fabric_name_device


# add server and unknown
add_visio_devices(server_shape_links_df, visio, stn, fabric_label_colours_dct)
# add storage and lib
add_visio_devices(storage_shape_links_df, visio, stn, fabric_label_colours_dct)
# create visio groups for switch Pairs
group_switch_pairs(san_graph_sw_pair_group_df, visio)

finish_time = f'finish: {current_datetime()}'
add_log_entry(log_file, '\n', finish_time, '^'*40, )


# doc = visio.ActiveDocument
# doc.MacrosEnabled
# doc.saveas(r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts\testsave.vsdm')
# doc.Close()

########################################################
# Functions


def visio_document_init(san_template_path, stencil_path, fabric_name_lst):
    """Fuction to initialize visio document with template, 
    add fabric_name pages and add page notes: customer, project title,
    fabric_name"""

    visio = win32com.client.Dispatch("Visio.Application")
    visio.Visible = 1
    visio.Documents.Add(san_template_path)
    stn = visio.Documents.OpenEx(stencil_path, 64)
    
    # rename first page
    visio.ActivePage.Name = fabric_name_lst[0]
    for fabric_name in fabric_name_lst[1:]:
        # duplicate and rename template page
        visio.ActivePage.Duplicate()
        visio.ActivePage.Name = fabric_name
    # add notes to the Document pages
    set_visio_page_note(visio)
    return visio, stn



def set_visio_page_note(visio, project_name=None):
    """Function to add page notes: customer name, project title, fabric_name"""

    doc = visio.ActiveDocument
    for page in doc.Pages:
        for shape in page.Shapes:
            if shape.Text == 'Fabric Name':
                shape.Text = 'Фабрика ' + page.Name
            elif project_name and shape.Text == 'Project title':
                shape.Text = project_name



def activate_visio_page(visio, page_name):
    """Function activates page with page_name in Visio document
    and returns this page object"""
    
    doc = visio.ActiveDocument
    page = doc.Pages.ItemU(page_name)
    visio.ActiveWindow.Page = page_name
    return page       


def add_visio_switch_shapes(san_graph_sw_pair_df, visio, stn):
    """Function to add swith and VC shapes to Visio document pages (fabric_name)""" 

    fabric_name_prev = None
    graph_level_prev = None
    x_group_current = 0
    
    add_log_entry(log_file, '\nSwitches\n', 'Fabric, switchName, switchWwn, swithNumber_in_pair, x-coordinate, y-coordinate')
        
    for idx, switch_pair_sr in san_graph_sw_pair_df.iterrows():
        # add_log_entry(log_file, '\n', switch_pair_sr.to_string())
        fabric_name_current = switch_pair_sr['Fabric_name']
        # graph_level: core, edge, ag
        graph_level_current = switch_pair_sr['graph_level']
        # activate page with switch_pair
        page = activate_visio_page(visio, page_name=fabric_name_current)
        # reset x-coordinate if new page (fabric_name) selected
        if fabric_name_current != fabric_name_prev:
            x_group_current = X_START
            fabric_name_prev = fabric_name_current
            
        # reset x-coordinate if shape drop level (y_graph_level) changed   
        if graph_level_current != graph_level_prev:
            x_group_current = X_START
            graph_level_prev = graph_level_current
        
        print("X: ", x_group_current)
        
        drop_switch_pair_shapes(switch_pair_sr, x_group_current, page, stn)
        # move cursor to the next switch pair location x-coordinate
        x_group_current =  x_group_current + switch_pair_sr['x_group_offset']
        


def drop_switch_pair_shapes(switch_pair_sr, x_group_current, page, stn):
    """Function to drop switch shapes from single switch_pair to the active visio page and
    add shape text (to all shapes of the switch_pair in case of directors and to 
    the bottom shape onlyk in case if switches and vcs"""


    # split switch_pair_srs details to build Visio graph
    master_shapes = switch_pair_sr['master_shape'].split(', ')
    shape_names = switch_pair_sr['switchName_Wwn'].split(', ')
    shape_text = switch_pair_sr['switchName_DID'].split('/ ')[::-1]
    
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
        
        print(switch_pair_sr['Fabric_name'], shape_name, i, x, y)
        
        # add enrty to visio graph creation log
        log_entry = ' '.join([switch_pair_sr['Fabric_name'], shape_name, str(i), str(x), str(y)])
        add_log_entry(log_file, log_entry)
        
        # add switch shape to the Visio page
        shape = page.Drop(master, x, y)
        shape.Name = shape_name
        
        # for Directors shape text is under each shape with
        # Director name, DID and model individually
        if 'DIR' in switch_pair_sr['switchClass_mode']:
            shape.Text = shape_text[i] + "\n" + switch_pair_sr['ModelName']
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




def current_datetime():
    """Function returns current datetime in 03/11/2022 11:37:45 format"""

    now = datetime.now()
    return now.strftime("%d/%m/%Y %H:%M:%S")


def add_log_entry(file_name, *args):
    """Function add lines (args) to the file_name"""
    
    # Open the file in append & read mode ('a+')
    with open(file_name, "a+") as file_object:
        appendEOL = False
        # Move read cursor to the start of file.
        file_object.seek(0)
        # Check if file is not empty
        data = file_object.read(100)
        if len(data) > 0:
            appendEOL = True
        # Iterate over each string in the list
        for log_entry in args:
            # If file is not empty then append '\n' before first line for
            # other lines always append '\n' before appending line
            if appendEOL == True:
                file_object.write("\n")
            else:
                appendEOL = True
            # Append element at the end of file
            file_object.write(log_entry)


def add_visio_inter_switch_connections(inter_switch_links_df, visio, stn, fabric_label_colours_dct):
    """Function to create inter switch shape connections"""
    
    # log entry header
    add_log_entry(log_file, '\nLinks\n', 'Fabric, switchName, switchWwn ----> Connected_switchName, Connected_switchWwn')
    
    for _, link_sr in inter_switch_links_df.iterrows():
        # add connection log entry
        log_entry =  ' '.join([link_sr['Fabric_name'], link_sr['shapeName'], '  ---->  ', link_sr['Connected_shapeName']])
        add_log_entry(log_file, log_entry)
        print(link_sr['shapeName'], '  ---->  ', link_sr['Connected_shapeName'])
        
        # activate page with shapes to be connected 
        fabric_name_current = link_sr['Fabric_name']
        page = activate_visio_page(visio, page_name=fabric_name_current)
        # drop connection between shapes
        drop_connector_shape(link_sr, page, stn, fabric_label_colours_dct, source='shapeName', destination='Connected_shapeName')




def add_visio_devices(san_links_df, visio, stn, fabric_label_colours):
    """Function to add servers, storages and libraries shapes to Visio document page and
    create connections between device shapes and switch shapes"""    

    
    fabric_name_prev = None
    x_coordinate = 0
    
    add_log_entry(log_file, '\nEdge devices')
    # find unique devices on fabric_name -> device_shapename -> device_class level
    san_device_shapes_df = find_device_shapes(san_links_df)
    
    print(san_device_shapes_df)
    
    for _, device_sr in san_device_shapes_df.iterrows():
        print(device_sr)
        add_log_entry(log_file, '\n', '-'*30, device_sr.to_string())
        fabric_name_current = device_sr['Fabric_name']
        # activate page with shape to be dropped 
        page = activate_visio_page(visio, page_name=fabric_name_current)
        
        # reset x-coordinate if new page (fabric_name) is activated
        if fabric_name_current != fabric_name_prev:
            x_coordinate = X_START
            fabric_name_prev = fabric_name_current
        
        print(fabric_name_current, device_sr['Device_shapeText'])   
        # drop shape on active page
        drop_device_shape(device_sr, page, stn, x_coordinate)
        # filter device_shapename links for the dropped shape
        # each row is switch -> device_shapename link with link quantity
        # device_shapename is single device or list of devices groped on fabric connection description
        shape_links_df = find_device_shape_links(device_sr, san_links_df)
        add_log_entry(log_file, shape_links_df.to_string())
        
        print(shape_links_df)
        for _, link_sr in shape_links_df.iterrows():
            # drop device_shapename -> switch links
            drop_connector_shape(link_sr, page, stn, fabric_label_colours, source='Device_shapeName', destination='switch_shapeName')    
        # shift x-coordinate to the right for the next device_shapename location
        x_coordinate =  x_coordinate + device_sr['x_group_offset']



def find_device_shapes(san_links_df):
    """Function to filter unique devices or device groups on 
    fabric_name -> device_shapename -> device_class level"""

    san_device_shapes_df = san_links_df.drop_duplicates(subset=['Fabric_name', 'Device_shapeName', 'deviceType']).copy()
    san_device_shapes_df = san_device_shapes_df[
        ['Fabric_name', 'Device_shapeText', 'Device_shapeName',  'deviceType', 'master_shape', 'y_graph_level', 'x_group_offset']]
    return san_device_shapes_df



def drop_device_shape(device_sr, page, stn, x_coordinate):
    """Function to drop shape on page with x_coordinate"""
    
    master = stn.Masters(device_sr['master_shape'])

    # drop page on page with x, y coordinates
    device_shape = page.Drop(master, x_coordinate, device_sr['y_graph_level'])
    device_shape.Name = device_sr['Device_shapeName']
    
    shape_text = device_sr['Device_shapeText']
    # synergy and blade server names are each on new line
    # replace ': ' and ', ' in device_shapetext with new line
    if device_sr['deviceType'] in ['SRV_BLADE', 'SRV_SYNERGY']:
        shape_text = re.sub(': |, ', '\n', shape_text)
        
    device_shape.Text = shape_text
    # device_shape.Cells("Char.Size").FormulaU = SHAPE_FONT_SIZE
    
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




def drop_connector_shape(link_sr, page, stn, fabric_label_colours_dct, source, destination):
    """Function to connect source and destination shapes from link_sr with connector_master shape.
    source and destination parameters are links_sr Series row titles with source and destination shapeNames accordingly"""

    source_shape_name = link_sr[source]
    destination_shape_name =  link_sr[destination]
    connector_master = stn.Masters('Link - HPE')
    
    # verify if source and desrinataion exist
    page_shape_name_lst = [page.Shapes.ItemU(i).Name  for i in range(1, page.Shapes.count + 1)]
    absent_shape_name_lst = [shape_name for shape_name in [source_shape_name, destination_shape_name] if shape_name not in page_shape_name_lst]
    
    if not absent_shape_name_lst:
        # sorce and destination shape objects
        source_shape = page.Shapes.ItemU(link_sr[source])
        destination_shape = page.Shapes.ItemU(link_sr[destination])
        # connect source and destionation shape objects (0 - connect without relocating the shapes)
        source_shape.AutoConnect(destination_shape, 0, connector_master)
        # last shape in the page shapes list is connector added above
        connector = page.Shapes.ItemU(page.Shapes.count)
        set_connector_attributes(connector, link_sr, fabric_label_colours_dct)
    else:
        print(f"Can't create link between {source_shape_name} and {destination_shape_name}. "
              f"{', '.join(absent_shape_name_lst)} missing on page {page.Name}")


def set_connector_attributes(connector, link_sr, fabric_label_colours_dct):
    """Function to set connector shape name, text, colour and lines number"""
    
    # connection shape colour
    fabric_label_colour = fabric_label_colours_dct[link_sr['Fabric_label']]
    link_quantity = link_sr['Physical_link_quantity']
    
    # set shape connector name
    connector.Name = link_sr['Link_shapeName']
    # set shape connector colour
    connector.Cells('LineColor' ).FormulaU = fabric_label_colour
    # set shape connector text (links number, trunk, npiv etc) if exist
    if pd.notna(link_sr['Link_description']):
        connector.Text = link_sr['Link_description']
    else:
        connector.Text = ""
    # connector.Cells("Char.Size").FormulaU = LINK_FONT_SIZE
    # set shape connector text colour
    connector.Cells('Char.Color' ).FormulaU = fabric_label_colour
    
    # show lines number for connector with links quantity up to four
    if  1 < link_quantity <= 4:
        connector.Cells("LinePattern").FormulaU = LINE_PATTERN[link_quantity]
    # for connector with links quantity greater than 4 show multiline connector
    elif link_quantity > 4:
        connector.Cells("LinePattern").FormulaU = LINE_PATTERN['4+']
            
            

def group_switch_pairs(san_graph_sw_pair_df, visio, ):
    """Function to create Visio groups for each switch pair"""
    
    add_log_entry(log_file, '\nSwitch groups')
    
    for idx, switch_pair_sr in san_graph_sw_pair_df.iterrows():
        add_log_entry(log_file, '\n', switch_pair_sr.to_string())
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
                print(shape_name)
                active_window.Select(page.Shapes.ItemU(shape_name), 2)
            # group all switches from switch_pair
            active_window.Selection.Group()
            # set name attribute for the group
            sw_group = page.Shapes.ItemU(len(page.Shapes))
            sw_group.Name = ' - '.join(shape_names)


##############################################################################################    
    # break

# san_graph_npiv_df.columns
# page = doc.Pages.ItemU("VZ")

# len(page.Shapes)
    
# page.Shapes.ItemU("xtau-fcsw2 10:00:50:eb:1a:3f:c2:12").NameU
    
# page.Shapes("Sheet.34").NameU

# page.Shapes.count
# visio.ActiveWindow.Page = "BB"    
    



# active_window = visio.ActiveWindow
# active_window.DeselectAll()

# active_window.Type
# # active_window.SelectAll()

# # active_window.Selection.SelectAll()
# # active_window.Selection.Group()


# page = visio.ActivePage

# active_window.Selection.Select(shape2, 2)

# # active_window.Select(shape1, 2)
# active_window.Select(shape2, 2)
# active_window.Select(shape3, 2)
# # active_window.Selection.ContainingShape
# active_window.Selection.Count

# sw_group = active_window.Selection.Group()


# master = stn.Masters("fcr_fd")
# shape = page.Drop(master, 100, 100)
# shape.Text = "FCR switch"
# shape.Cells("Char.Size").FormulaU = SHAPE_FONT_SIZE

# shape_tst = page.Shapes.ItemU("dr03av07-r2-228 10:00:88:94:71:7e:7d:88")
# len(shape_tst.Text)
# shape_chars = shape_tst.Characters

# shape_chars.Begin = 0
# shape_chars.End = 15
# shape_chars.Cells("CharProps").FormulaU = 2

# shape_chars.CharProps(2, 1)

# bool(switch_pair['ModelName'])

# for i, a, b in zip(range(len(master_shapes)), ['a', 'b', 'c'], [1,2,3]):
#     print(i, a,b)
    
# list(range(2, -1, -1))