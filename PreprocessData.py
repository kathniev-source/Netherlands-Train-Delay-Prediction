import pandas as pd
#function to read data
def combine_data(data):
    rainfall_data = data[0]
    temperature_data = data[1]
    old_data = data[2]
    new_data = old_data.merge(rainfall_data, right_on="Date", left_on="Current_Date", validate = 'm:1')
    new_data = new_data.merge(temperature_data,  right_on="Date", left_on="Current_Date", validate = 'm:1')
    new_data.drop(columns=['Date_x','Date_y'], inplace=True)

    print(new_data.columns)
    data.append(new_data)

def main():
    path_to_rainfall_data = 'C:/Users/chieu/Desktop/Data/Separate a single trip/rainfall.csv'
    path_to_temperature_data = 'C:/Users/chieu/Desktop/Data/Separate a single trip/temperature.csv'
    path_to_combined_data = 'C:/Users/chieu/Desktop/Data/Separate a single trip/Combined-Train-TU-One-Station-200060.csv'

    data = []
    rainfall_data = pd.read_csv(path_to_rainfall_data, header=0, dtype = object)
    temperature_data = pd.read_csv(path_to_temperature_data, header=0, dtype = object)
    old_data = pd.read_csv(path_to_combined_data, header=0, dtype = object)
    data.append(rainfall_data)
    data.append(temperature_data)
    data.append(old_data)

    combine_data(data)
    preprocessed_data = data[3]
    preprocessed_data.to_csv("Preprocess_data.csv", index=False, header=True, mode='a', na_rep='NA')

    
if __name__ == "__main__":
    main()

