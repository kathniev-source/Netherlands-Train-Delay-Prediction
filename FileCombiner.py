from decimal import Decimal
import numpy as np
import pandas as pd
import datetime
import os
from os.path import join
import re

#an array to store date of trip_updates files
trip_updates_paths = []
current_date = [""]
log_content = []
# used in log function to highlight in green color
def highlight_with_colour(text, colour):
    if(colour == "red"):
        return '\033[31m' + text + "\033[0m"
    elif(colour == "green"):
        return '\033[32m' + text + "\033[0m"

#time_stamp in log function
def log_timestamp(text, func_name, time, start_time=None):
    if start_time is None:  # used for specifying the start time
        print(highlight_with_colour(text, "green") + ' ' + highlight_with_colour(func_name + '(): ', "red") + str(time))
    else:  # used for specifying the finish time & time period
        print(highlight_with_colour(text, "green") + ' ' + highlight_with_colour(func_name + '(): ', "red") + str(time) + ', spent ' +
              highlight_with_colour(str(time - datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f')), "red"))

# log function can print the start & end time of other funcs
def log(func):
    def wrapper(*args, **kw):
        name = func.__name__
        start_time = datetime.datetime.now()
        log_timestamp('Start', name, start_time)  # record the start time for a function
        #return the function processed and show the time and period of the function
        return func(*args, **kw), \
               log_timestamp('Finish', name, datetime.datetime.now(), start_time=str(start_time))
    return wrapper

#Used to generate directories for Combined Data & Log
def create_dir(path):
    if os.path.exists('./' + path + '/'):
        pass
    else:
        os.mkdir('./' + path + '/')
    combined_path = './' + path + '/'
    return combined_path

@log
def initialise_combined_daily_data_list(path_to_cleaned_data):
    log_content.append(str(datetime.datetime.now()) + " - Started initialising cleaned data at " + str(datetime.datetime.now()))
    # Walk through all of the data file inside of the folder and get all of the timestamp out so we can match with the schedule time later
    for root, dirs, files in os.walk(path_to_cleaned_data):
        for file_name in files:
            if not ".csv" in file_name:
                continue
            trip_updates_paths.append(join(root,file_name))
    log_content.append(str(datetime.datetime.now()) + " - Ended initialising cleaned data at " + str(datetime.datetime.now()))

#walk through all combined data csv files
@log
def get_combined_daily_data(data):
    if(len(trip_updates_paths) == 0):
        print("There is no more file to combine!")
        exit(-1)
    path = trip_updates_paths.pop(0)
    log_content.append( str(datetime.datetime.now()) + " - Started opening realtime data at '" + path + "' (" + str(os.path.getsize(path)) + " byte(s)) . . .")
    realtime_data = pd.read_csv(path, header=0, dtype = object)
    data.append(realtime_data)
    log_content.append(str(datetime.datetime.now()) + " - Ended  opening realtime data at '" + path + "' (" + str(os.path.getsize(path)) + " byte(s)) . . .")

# save the data into a file with format filename--hour.csv
@log
def save_data(modified_data, file_name, cleaned_path):  # save the data into a file with format filename--hour.csv
	cleaned_dir = create_dir(cleaned_path)
	filename = cleaned_dir + file_name + '-- Combined--final--ver3.csv'
	if os.path.exists(filename):  # if the file exists, write without header
		modified_data.to_csv(filename, index=False, header=False, mode='a', na_rep='NA')
	else:  # if the file not exists, write with header
		modified_data.to_csv(filename, index=False, header=True, mode='a', na_rep='NA')
	del modified_data

def main():
    if os.path.exists('./Combined Train TU Data/'):
        pass
    else:
        create_dir('Combined Train TU Data')
    if os.path.exists('./Combined Train TU Log/'):
        pass
    else:
        create_dir('Log')
    pd.set_option('display.max_columns', None)

    initialise_combined_daily_data_list(r"C:/Users/chieu/Desktop/Combined Train TU Data6")
    data = []
    while(len(trip_updates_paths)!=0):
        get_combined_daily_data(data)
    concatenated_dataframe = pd.concat(data)
    save_data(concatenated_dataframe, "Train-TU", "./Combined Train TU Data/")
    with open('./Log/Combined Train TU Log.txt', 'a+', encoding='utf8', newline="") as fo:
        for logged_event in log_content:
            fo.write(logged_event + "\n")

if __name__ == "__main__":
    main()
