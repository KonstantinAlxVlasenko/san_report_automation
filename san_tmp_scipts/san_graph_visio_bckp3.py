# -*- coding: utf-8 -*-
"""
Created on Tue Nov  8 10:29:28 2022

@author: kavlasenko
"""

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


script_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)


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




doc = visio.ActiveDocument
# page = visio.ActiveWindow
# for shape in page.Shapes:
#     print(shape.Text)


for page in doc.Pages:
    for shape in page.Shapes:

        print(shape.Text)

        if shape.Text == 'Customer / SAN Assessment':
            shape.Text = 'Data Line' + ' / ' + 'SAN Inventory'


for page in doc.Pages:
    for shape in page.Shapes:
        if shape.Text == 'Fabric Name':
            shape.Text = 'Фабрика ' + page.Name
        elif project_name and shape.Text == 'Project title':
            shape.Text = project_name




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
        
    for idx, switch_pair in san_graph_sw_pair_df.iterrows():
        
        # add_log_entry(log_file, '\n', switch_pair.to_string())
        
        fabric_name_current = switch_pair['Fabric_name']
        # graph_level: core, edge, ag
        graph_level_current = switch_pair['graph_level']
        
        # doc = visio.ActiveDocument
        # page = doc.Pages.ItemU(fabric_name_current)
        # visio.ActiveWindow.Page = fabric_name_current
        
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
        
        # split switch_pairs details to build Visio graph
        master_shapes = switch_pair['master_shape'].split(', ')
        shape_names = switch_pair['switchName_Wwn'].split(', ')
        shape_text = switch_pair['switchName_DID'].split('/ ')[::-1]
        
        # first drop second switch of the switch_pair to make the first switch visually overlap to the second switch 
        # two ENT swtiches sw1 and sw2 -> [(0, 'ENT', 'sw2'), (1, 'ENT', 'sw1')]
        for i, master_shape, shape_name in list(zip(range(len(master_shapes)-1, -1, -1), master_shapes, shape_names))[::-1]:
            
            # choose shape icon
            master = stn.Masters(master_shape)
            # set x, y coordinates
            # x_shape_offset is for Direcors only (for all other devices it's 0)
            x = x_group_current - i * switch_pair['x_shape_offset']
            # y_shape_offset makes fabic_label A switch shape to be located above fabic_label B switch shape
            y = switch_pair['y_group_level'] + i * switch_pair['y_shape_offset']
            
            print(fabric_name_current, shape_name, i, x, y)
            
            # add enrty to visio graph creation log
            log_entry = ' '.join([fabric_name_current, shape_name, str(i), str(x), str(y)])
            add_log_entry(log_file, log_entry)
            
            # add switch shape to the Visio page
            shape = page.Drop(master, x, y)
            shape.Name = shape_name
            
            # for Directors shape text is under each shape with
            # Director name, DID and model individually
            if 'DIR' in switch_pair['switchClass_mode']:
                shape.Text = shape_text[i] + "\n" + switch_pair['ModelName']
            else:
                shape.Text = " "
        
        # for all switches except Directors shape text with switch name, DID and model 
        # for the switch pair on the bottom shape of the switch pair
        if not 'DIR' in switch_pair['switchClass_mode']:
            bottom_shape = page.Shapes.ItemU(shape_names[-1])
            if switch_pair['ModelName']:
                bottom_shape.Text = switch_pair['switchName_DID'] + "\n" + switch_pair['ModelName']
            else:
                bottom_shape.Text = switch_pair['switchName_DID']
        
        # move cursor to the next switch pair location x-coordinate
        x_group_current =  x_group_current + switch_pair['x_group_offset']
        




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
        # doc = visio.ActiveDocument
        # page = doc.Pages.ItemU(fabric_name_current)
        # visio.ActiveWindow.Page = fabric_name_current
        page = activate_visio_page(visio, page_name=fabric_name_current)
        # drop connection between shapes
        create_two_shapes_connection(link_sr, page, stn, fabric_label_colours_dct, source='shapeName', destination='Connected_shapeName')




def add_visio_devices(san_links_df, visio, stn, fabric_label_colours):
    """Function to add servers, storages and libraries to Visio document pages and
    create cnnections between device shapes and switch shapes"""    


    

    fabric_name_prev = None
    x_coordinate = 0
    
    add_log_entry(log_file, '\nEdge devices')
    san_device_shapes_df = find_device_shapes(san_links_df)
    
    print(san_device_shapes_df)
    
    for _, device_sr in san_device_shapes_df.iterrows():
        
        
        add_log_entry(log_file, '\n', '-'*30, device_sr.to_string())
        
        # if link['Fabric_name'] != "BB":
        #     continue
        
        fabric_name_current = device_sr['Fabric_name']
        print(device_sr)
        
        
        doc = visio.ActiveDocument
        page = doc.Pages.ItemU(fabric_name_current)
        visio.ActiveWindow.Page = fabric_name_current   
        
        if fabric_name_current != fabric_name_prev:
            x_coordinate = X_START
            fabric_name_prev = fabric_name_current
        
        print(fabric_name_current, device_sr['Device_shapeText'])   
        
        drop_device_shape(device_sr, page, stn, x_coordinate)

        shape_links_df = find_device_shape_links(device_sr, san_links_df)
        add_log_entry(log_file, shape_links_df.to_string())
        
        print(shape_links_df)
        
        for _, link_sr in shape_links_df.iterrows():
            
            
            create_two_shapes_connection(link_sr, page, stn, fabric_label_colours, source='Device_shapeName', destination='switch_shapeName')    
        
        x_coordinate =  x_coordinate + device_sr['x_group_offset']


def find_device_shapes(san_links_df):
    """Function to locate unique devices out of Device Links DataFrame"""

    # create storage, library unique df to iterate
    san_device_shapes_df = san_links_df.drop_duplicates(subset=['Fabric_name', 'Device_shapeName', 'deviceType']).copy()
    san_device_shapes_df = san_device_shapes_df[['Fabric_name', 'Device_shapeText', 'Device_shapeName',  'deviceType', 'master_shape', 'y_graph_level', 'x_group_offset']]
    return san_device_shapes_df



def drop_device_shape(device_sr, page, stn, x_coordinate):
    """Function to drop shape on page with x_coordinate"""
    
    master = stn.Masters(device_sr['master_shape'])

    # drop page on page with x, y coordinates
    device_shape = page.Drop(master, x_coordinate, device_sr['y_graph_level'])
    device_shape.Name = device_sr['Device_shapeName']
    
    shape_text = device_sr['Device_shapeText']
    # synergy and blade server names are each on new line
    if device_sr['deviceType'] in ['SRV_BLADE', 'SRV_SYNERGY']:
        shape_text = re.sub(': |, ', '\n', shape_text)
        
    device_shape.Text = shape_text
    device_shape.Cells("Char.Size").FormulaU = SHAPE_FONT_SIZE
    
    # # make title bold
    # shape_chars = device_shape.Characters
    # device_shape.CharProps(2, 1)
    return device_shape


def find_device_shape_links(device_sr, san_links_df):
    """Function to locate all current device_sr shape links to switch shapes in the fabric_name"""

    mask_fabric_name = san_links_df['Fabric_name'] == device_sr['Fabric_name']
    mask_storage_name = san_links_df['Device_shapeName'] == device_sr['Device_shapeName']
    mask_device_type = san_links_df['deviceType'] == device_sr['deviceType']
    return san_links_df.loc[mask_fabric_name & mask_storage_name & mask_device_type]




def create_two_shapes_connection(link_sr, page, stn, fabric_label_colours_dct, source, destination):
    """Function to connect source and destination shapes with connector_master.
    source, destination - links_sr title with source and destination shapeName accordingly"""

    
    fabric_label_colour = fabric_label_colours_dct[link_sr['Fabric_label']]
    source_shape_name = link_sr[source]
    destination_shape_name =  link_sr[destination]
    link_quantity = link_sr['Physical_link_quantity']
    
    connector_master = stn.Masters('Link - HPE')
    
    page_shape_name_lst = [page.Shapes.ItemU(i).Name  for i in range(1, page.Shapes.count + 1)]
    absent_shape_name_lst = [shape_name for shape_name in [source_shape_name, destination_shape_name] if shape_name not in page_shape_name_lst]
    
    if not absent_shape_name_lst:

        
        source_shape = page.Shapes.ItemU(link_sr[source])
        destination_shape = page.Shapes.ItemU(link_sr[destination])
        
        source_shape.AutoConnect(destination_shape, 0, connector_master)
        connector = page.Shapes.ItemU(page.Shapes.count)
        connector.Name = link_sr['Link_shapeName']
        connector.Cells('LineColor' ).FormulaU = fabric_label_colour
        if pd.notna(link_sr['Link_description']):
            connector.Text = link_sr['Link_description']
        else:
            connector.Text = ""
        # connector.Cells("Char.Size").FormulaU = LINK_FONT_SIZE
        connector.Cells('Char.Color' ).FormulaU = fabric_label_colour
        
        
        if  1 < link_quantity < 5:
            connector.Cells("LinePattern").FormulaU = LINE_PATTERN[link_quantity]
        
        elif link_quantity > 4:
            connector.Cells("LinePattern").FormulaU = LINE_PATTERN['4+']
    else:
        print(f"Can't create link betwen {source_shape_name} and {destination_shape_name}. {', '.join(absent_shape_name_lst)} missing on page {page.Name}")


def group_switch_pairs(san_graph_sw_pair_df, visio, ):
    """Function to create Visio groups for each switch pair"""
    
    
    add_log_entry(log_file, '\nSwitch groups')
    

    
    for idx, switch_pair in san_graph_sw_pair_df.iterrows():
        
        add_log_entry(log_file, '\n', switch_pair.to_string())
        
        fabric_name_current = switch_pair['Fabric_name']
        
        doc = visio.ActiveDocument 
        page = doc.Pages.ItemU(fabric_name_current)
        visio.ActiveWindow.Page = fabric_name_current
        active_window = visio.ActiveWindow
        
        
        shape_names = switch_pair['switchName_Wwn'].split(', ')
        
        # create switch_pair groups
        active_window.DeselectAll()
        for shape_name in shape_names:
            print(shape_name)
            active_window.Select(page.Shapes.ItemU(shape_name), 2)
        active_window.Selection.Group()
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