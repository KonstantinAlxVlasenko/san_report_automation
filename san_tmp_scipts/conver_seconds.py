# -*- coding: utf-8 -*-
"""
Created on Wed Dec 27 01:08:17 2023

@author: kavlasenko
"""

def convert_seconds(seconds):
    """
    Asks the user to enter a number of seconds and displays the total time entered in days, hours, minutes, and seconds.
    """
    
    # Store the initial number of seconds
    initial_seconds = seconds
    # Calculate the number of days, hours, minutes, and seconds
    days = seconds // (24 * 60 * 60)
    seconds %= (24 * 60 * 60)
    hours = seconds // (60 * 60)
    seconds %= (60 * 60)
    minutes = seconds // 60
    seconds %= 60

    # Create a list to store the time units
    time_units = []

    # Add non-zero time units to the list
    if days > 0:
        time_units.append(f"{days} day(s)")
    if hours > 0:
        time_units.append(f"{hours} hour(s)")
    if minutes > 0:
        time_units.append(f"{minutes} minute(s)")
    if seconds > 0:
        time_units.append(f"{seconds} second(s)")

    # Format the time units with proper punctuation
    if len(time_units) == 0:
        time_string = "less than one second"
    elif len(time_units) == 1:
        time_string = time_units[0]
    else:
        time_string = ", ".join(time_units[:-1]) + ", and " + time_units[-1]

    # Display the result
    print(f"{initial_seconds:,} seconds equals {time_string}.")
    
    
convert_seconds(476043)
convert_seconds(124)