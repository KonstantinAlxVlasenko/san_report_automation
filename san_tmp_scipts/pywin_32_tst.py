# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 18:12:36 2022

@author: vlasenko
"""

import win32com.client

# excel = win32com.client.Dispatch('Excel.Application')

# excel.Visible = True
# _ = input("Press ENTER to quit:")

# excel.Application.Quit()

word_app = win32com.client.Dispatch('Word.Application')

word_app.Visible = True
word_app.ListCommands(ListAllCommands=True)
# word_app.Documents.Add()
print(word_app.ActiveDocument)


_ = input("Press ENTER to quit:")

word_app.Application.Quit()