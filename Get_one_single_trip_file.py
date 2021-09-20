import pandas as pd

def main():
    path_to_file = 'C:/Users/chieu/Desktop/Data/Combined Train TU Data/Train-TU-- Combined.csv'
    path_to_save = 'C:/Users/chieu/Desktop/Data/Separate a single trip/Combined-Train-TU-One-Station'
    station = '200060'

    #service_id = '1479.121.60.K.8.59461388'
    # read csv
    # declare a new dataframe
    try:
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        test_frame = pd.read_csv(path_to_file, header=0, dtype=object, infer_datetime_format = True, date_parser= True)

    except:
        print('Path to file is not correct, please check again!')
    # iterate all row to find the matching trip_number and service_id
    # copy the matching row and append to a new dataframe
    try:
        #found_frame = test_frame.loc[(test_frame['Trip_Number'] == trip_num) & (test_frame['Service_ID'] == service_id)]
        found_frame = test_frame.loc[test_frame['Parent_Station_Code'] == station]

    except ValueError:
        print('Invalid values detected in ' + path_to_file)

    if len(found_frame)==0:
        print('CSV file is empty or search strings are invalid, cannot export to file!')
    else:
        path_to_save += '-' + station + '.csv'
        # Can replace this by using the save function in the combiner
        found_frame.to_csv(path_to_save, index=False, header=True, mode='a', na_rep='NA')
        print('Exported to ' + path_to_save)


if __name__ == "__main__":
    main()

