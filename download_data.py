import csv
import os
from unzip import unzip
import sys
import pandas as pd
import datetime

#command line arguments
symbol_ = sys.argv[1]
interval = sys.argv[2]
year = sys.argv[3]
directory_name = sys.argv[4]

# downloading klines data
os.system("python download_kline.py -t um -s " + str(symbol_) + " -y " + str(year) + " -i " + str(interval))

#unzipping files
directory = "data/futures/um/monthly/klines/" + str(symbol_) + "/" + str(interval)
unzip(directory)

#making the directory if it doesn't exist
def makedir(directory):
    if os.path.exists(directory):
        pass
    else:
        os.makedirs(directory)

#month names 
MONTH_DICT = {"01" : "Jan", "02" : "Feb", "03" : "Mar", "04" : "Apr", "05" : "May", "06" : "Jun", "07" : "Jul", "08" : "Aug", "09" : "Sep", "10" : "Oct", "11" : "Nov", "12" : "Dec"}
month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

#function for adding header and formatting 
def unix_to_utc(input_col_name):
    output_col_name = []
    for time in input_col_name:
        time_int = int(time)
        unix_time = time_int/1000
        time_utc = datetime.datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
        output_col_name.append(str(time_utc))
    return output_col_name

def add_header(filename):
    df = pd.read_csv(filename)
    unix_start = df.iloc[:, 0]
    utc_start = unix_to_utc(unix_start)
    open_price = df.iloc[:,1].to_list()
    high_price = df.iloc[:,2].to_list()
    low_price = df.iloc[:,3].to_list()
    close_price = df.iloc[:, 4].to_list()
    volume_btc = df.iloc[:,10].to_list()
    volume_usdt = df.iloc[:,11].to_list()
    number_of_trades = df.iloc[:,9].to_list()

    file_name = filename[:-4]    
    file_name = file_name.split("-")
    symbol = symbol_
    date = file_name[2]
    month = file_name[3]
    # print(month,date)

    new_df = pd.DataFrame()

    new_df["tradingDay"] = utc_start
    new_df["unix"] = unix_start
    new_df["symbol"] = symbol
    new_df["Open"] = open_price
    new_df["High"] = high_price
    new_df["Low"] = low_price
    new_df["Close"] = close_price
    new_df["Volume BTC"] = volume_btc
    new_df["Volume USDT"] = volume_usdt
    new_df["tradecount"] = number_of_trades 
    
    print("Shape of new data frame is :", new_df.shape) # This itself is showing that this is not a usable dataframe. 
    new_df.to_csv(str(directory_name) + "/M_" + str(MONTH_DICT[month]) + "_" + str(date) + "_complete_binance_" + symbol + ".csv", index = False) 

#formatting the extracted files and adding header
def formatting():
    makedir(directory_name)
    for i in os.listdir("data/futures/um/monthly/klines/" + str(symbol_) + "/" + str(interval)):
        if i.endswith(".csv"):
            add_header("data/futures/um/monthly/klines/" + str(symbol_) + "/" + str(interval) + "/" + i)

formatting() # run this if your zip files exist in this directory only

#rows array to store the data of formatted files
rows = [] 

#getting the data
for i in month_names:
    try:
        with open(directory_name + "/M_" + i + "_" + year + "_" + "complete_binance_" + symbol_ + ".csv", "r") as file:
            reader = csv.reader(file)
            for i in reader:
                rows.append(i)

    except:
        print(f"FILE NOT FOUND, check for the file of {i}-{year}")

#writing final csv file
with open("yearly_data.csv", "w") as csv_file:
    writer = csv.writer(csv_file)
    header = ["tradingDay","unix","symbol","Open","High","Low","Close","Volume BTC","Volume USDT","tradecount"]
    writer.writerow(header)
    writer.writerows(rows)