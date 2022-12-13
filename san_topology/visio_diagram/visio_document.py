

import os

import pandas as pd
import win32com.client

import utilities.module_execution as meop
from san_automation_constants import LEFT_INDENT


def visio_document_init(visio_template_path, visio_stencil_path, fabric_name_lst, report_requisites_sr):
    """Fuction to initialize visio document with template, 
    add fabric_name pages and add page notes: customer, project title,
    fabric_name"""

    visio = win32com.client.Dispatch("Visio.Application")
    visio.Visible = 1
    visio.Documents.Add(visio_template_path)
    stn = visio.Documents.OpenEx(visio_stencil_path, 64)
    
    # rename first page
    visio.ActivePage.Name = fabric_name_lst[0]
    for fabric_name in fabric_name_lst[1:]:
        # duplicate and rename template page
        visio.ActivePage.Duplicate()
        visio.ActivePage.Name = fabric_name
    # add notes to the Document pages
    set_visio_page_note(visio, customer=report_requisites_sr['customer_name'], 
                                project=report_requisites_sr['project_title'])
    return visio, stn



def set_visio_page_note(visio, customer=None, project=None):
    """Function to add page notes: customer name, project title, fabric_name"""

    doc = visio.ActiveDocument
    for page in doc.Pages:
        for shape in page.Shapes:
            if shape.Text == 'Description':
                shape.Text = page.Name
            elif all((customer, project, shape.Text=='Customer / Project')):
                shape.Text = customer.replace('_', ' ') + ' / ' + project.replace('_', ' ')
            
            elif customer and shape.Text == 'Customer / Project':
                shape.Text = customer.replace('_', ' ')


def activate_visio_page(visio, page_name):
    """Function activates page with page_name in Visio document
    and returns this page object"""
    
    doc = visio.ActiveDocument
    page = doc.Pages.ItemU(page_name)
    visio.ActiveWindow.Page = page_name
    return page


def save_visio_document(visio, report_requisites_sr, current_datetime=meop.current_datetime(join=True)):

    if not visio:
        return pd.Series(name='visio_file', dtype='object')

    doc = visio.ActiveDocument
    file_name = report_requisites_sr['customer_name'] + '_SAN_topology_' + current_datetime + '.vsdm'
    file_path = os.path.join(report_requisites_sr['today_report_folder'], file_name)
    doc.saveas(file_path)
    return pd.Series([file_name], name='visio_file')




    
def get_tqdm_desc_indented(tqdm_desc_str, max_desc_len):
    return ' '*LEFT_INDENT + tqdm_desc_str.ljust(max_desc_len)



