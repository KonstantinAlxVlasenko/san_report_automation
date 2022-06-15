# -*- coding: utf-8 -*-
"""
Created on Thu Aug  6 23:54:16 2020

@author: vlasenko
"""
import win32com.client
import os
script_dir = r'C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\05.PYTHON\Projects\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)

visio = win32com.client.Dispatch("Visio.Application")

visio.Visible = 1


# visio = win32.gencache.EnsureDispatch('Visio.Application')

# visio = win32.Dispatch("Visio.Application")

# document = visio.Documents.Add("")
# #
# page = visio.ActivePage
# #
# shapes = page.Shapes

# visio.ActiveWindow


san_template = r'C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\05.PYTHON\Projects\san_tmp_scipts\SAN_Drawings_template.vstm'
doc = visio.Documents.Add(san_template)

# visio.Documents.GetNames()


# visio.ActivePage.Duplicate()

# visio.ActiveDocument.OpenStencilWindow()

# visio.Documents.GetNames()

# visio.ActivePage.Shapes.Count

# visio.ActiveDocument.SaveAs(r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\05.PYTHON\Projects\san_tmp_scipts\san_dr.vsdm")
# visio.ActiveDocument.Close()


visio.ActivePage.Name = "SAN2"

visio.ActiveWindow.ViewFit = 1

doc.Pages.Add()
visio.ActivePage.Name = "SAN2"

doc.Pages.ItemU('SAN1').Name




visio.Quit()

visio.ActivePage


page = doc.Pages.ItemU('DR')

visio.ActiveWindow.Page = page

page = visio.ActivePage

page.Name = 'san'

stn = visio.Documents.OpenEx("san_assessment_mod.vssx", 64)
# stn = visio.Documents.Open("san_assessment.vssx")

# visio.ActiveWindow
# visio.ActivePage


# master.GetNames

page = doc.Pages.ItemU('SAN1')


master = stn.Masters('dir_8slot')
shape1 = page.Drop(master, 92, 67)
# shape1.Title = 'shape_tst'
shape1.Text = "Director\nLine"
shape1.Cells("Char.Color").FormulaU = 'RGB(0,169,103)'


master = stn.Masters('vc')
shape_vc = page.Drop(master, 202, 67)
shape_vc.Text = 'Test VC'
shape_vc.Name = 'VC 1'
bottom_shape = page.Shapes.ItemU('VC 1')
bottom_shape.Text = 'VC 1\nbbnreg'



shape1.Cells("Char.Size").FormulaU = "40 pt"
shape1.Name = 'SHAPE'

page.Shapes.ItemU('SHAPE')


master = stn.Masters('sw_enterprise')
shape2 = page.Drop(master, 60, 105)
shape2.Text = "switch"

master = stn.Masters('mediumsw')
shape3 = page.Drop(master, 3, 9.7)
shape3.Text = "Switch2"

master = stn.Masters('sw_embedded')
shape4 = page.Drop(master, 20, 122)
shape4.Title = "Switch3"

master = stn.Masters('virtual connect')
shape4 = page.Drop(master, 50, 127)

master = stn.Masters('sw_middle')
shape5 = page.Drop(master, 100, 90)
shape5.Text = "Switch5"


master = stn.Masters('sw_entry')
shape6 = page.Drop(master, 140, 100)
shape6.Text = 'TEST_SWITCH'
shape6.Name = 'sw_entry_tst'
shape6.Cells("Char.Size").FormulaU = "12 pt"
shape6.Cells("Char.Color").FormulaU = 'RGB(0,169,103)'


master = stn.Masters('fcr_fd')
shape5 = page.Drop(master, 180, 90)

active_window = visio.ActiveWindow
active_window.DeselectAll()

active_window.Type
# active_window.SelectAll()

# active_window.Selection.SelectAll()
# active_window.Selection.Group()


page = visio.ActivePage

active_window.Selection.Select(shape2, 2)

# active_window.Select(shape1, 2)
active_window.Select(shape2, 2)
active_window.Select(shape3, 2)
# active_window.Selection.ContainingShape
active_window.Selection.Count

sw_group = active_window.Selection.Group()
sw_group.Text = "Switch group"
# active_window.Select(page.Shapes.ItemU("mediumsw"), 2)



connectorMaster = stn.Masters('Link - HP E')


connector12 = page.Drop(connectorMaster, 0, 0)

shape_a = page.Shapes.ItemU('SHAPE')

connector12 = shape_a.AutoConnect(shape2,0, connectorMaster)


connector12 = shape1.AutoConnect(shape2,0, connectorMaster)
connector12 = page.Shapes.ItemU(len(page.Shapes))

connector12.Text = ''
shape1.Cells["Char.Size"].FormulaForceU = "RGB(255,255,255)"

shape1


connector12.Text
connector12.Cells["Char.Color"].FormulaU = 'RGB(0,169,103)'
connector12.Cells( 'LineColor' ).FormulaU = 'RGB(0,169,103)'
connector12.Cells( 'Char.Color' ).FormulaU = 'RGB(0,169,103)'
connector12.Cells("Char.Size").FormulaU = "12 pt"

connector12.Text




connector13 = shape1.AutoConnect(shape3,0, connectorMaster)

type(connector12)


connector12.Cells("LinePattern").FormulaU = 'USE("Parallel Lines x4")'

connector = page.Shapes.ItemU(len(page.Shapes))

page.Shapes.ItemU(5).Name

connector.Cells("LineColor").FormulaForce = 3
connector.Text
# connectorMaster = visio.Application.ConnectorToolDataObject

connector = page.Drop(connectorMaster, 0, 0)
connector.Cells("BeginX").GlueTo(shape1.Cells("PinX"))
connector.Cells("EndX").GlueTo(shape2.Cells("PinX"))
connector.Cells( 'LineColor' ).FormulaU = 'RGB(0,0,255)'
connector.Cells('ShapeRouteStyle').FormulaU = '2'


# help(connector)

connector = page.Drop(connectorMaster, 0, 0)
connector.Cells("BeginX").GlueTo(shape1.Cells("PinX"))
connector.Cells("EndX").GlueTo(shape2.Cells("PinX"))
connector.Cells( 'LineColor' ).FormulaU = 'RGB(255,0,0)'
connector.Cells('ShapeRouteStyle').FormulaU = '2'