# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 01:33:37 2022

@author: vlasenko
"""


	
import win32com.client as win32
#
# visio = win32.gencache.EnsureDispatch('Visio.Application')

visio = win32.Dispatch("Visio.Application")

document = visio.Documents.Add("")
#
page = visio.ActivePage
#
shapes = page.Shapes


page.Shapes
	

rect1 = page.DrawRectangle(1,1,2,2)
rect2 = page.DrawRectangle(4,4,5,5)
oval1 = page.DrawOval(1,4,2,5)
oval2 = page.DrawOval(4,1,5,2)

rect1.Text="Rect1"
rect2.Text="Rect2"
oval1.Text="Oval1"
oval2.Text="Oval2"

active_window = visio.ActiveWindow



active_window.Selection.Select(rect1, 2)
active_window.Selection.Select(oval1, 2)


active_window.Selection.SelectAll()
active_window.Selection.Group()