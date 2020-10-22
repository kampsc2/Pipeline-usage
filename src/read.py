import os
import pandas as pd
import datetime

def get_crude_data(start = 2020):
    
    datasets = [x for x in os.listdir('data/capacity/') if '.csv' in x]
    
    # read all of the data from the static csvs
    df = pd.DataFrame()
    for x in datasets:
        # some weird encoding on some of the files
        try:
            temp = pd.read_csv(f'data/capacity/{x}', encoding = 'utf-8')
        except UnicodeDecodeError:
            temp = pd.read_csv(f'data/capacity/{x}', encoding = 'latin-1')
        
        # determine if this is an oil pipeline or a natural gas pipeline
        if "Date" in temp.columns:
            pass # natural gas contains a date column because it is measured daily instead of monthly
        else:
            # only consider crude oil
            df = df.append(temp) 
    
    
    # Drop empty columns
    df = df.dropna(how = 'all', axis = 1)
        
    # rename some columns to make it easier to work with
    df['capacity'] = df['Available Capacity (1000 m3/d)']
    df['throughput'] = df['Throughput (1000 m3/d)']
    df['date'] = df.apply(lambda x: datetime.date(x.Year, x.Month, 1), axis = 1)
    
    # lets consider only since the start date
    df = df[df.Year >= start]
    
    return df
