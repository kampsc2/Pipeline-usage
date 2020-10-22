import pandas as pd
import os
import datetime
import numpy as np

def make_analysis_table(df, date = datetime.date(2020,2,1), show_trade_type = False, return_type = 'table'):
    df = df[df.date == date].copy()
    
    # calculate the capacity data for each of the pipelines
    capacity = pd.DataFrame()
    for pipeline in df['Pipeline Name'].unique():
    
        # Pull out the capacity data by selecting some header columns
        cap_data = df[df['Pipeline Name'] == pipeline]
        cap_data = cap_data[['Pipeline Name','Key Point','capacity']].drop_duplicates()
        cap_data = cap_data.groupby(['Pipeline Name','Key Point'], dropna = True).agg({'capacity':'sum'}).reset_index()
        
        # special case where the total capacity is not distributed amongst the key points
        if pipeline == 'Trans Mountain Pipeline':
            cap_data['capacity'] = cap_data['capacity']/3
        
        capacity = capacity.append(cap_data)
    

    # creating a totals row which sums up all of the trade types
    total = df.copy()
    total['Trade Type'] = 'Total'
    df = df.append(total)
        
    # group by the pipline, key point, and trade type then pivot out into a nice table
    df = df.groupby(['Pipeline Name','Key Point','Trade Type']).agg({'throughput':'sum'}).reset_index()
    df = df.pivot(index= ['Pipeline Name','Key Point'], columns = 'Trade Type',values = 'throughput').reset_index()
    df = df.sort_values(['Pipeline Name']).set_index(['Pipeline Name','Key Point'])
        
    # joing the pivot table with the capacity data
    df = df.merge(capacity, on = ['Pipeline Name','Key Point'])
    
    # Calculate the capacity margin 
    df['Margin'] = (df['capacity'] - df['Total']) / df['capacity']
    df.loc[~np.isfinite(df['Margin']), 'Margin'] = np.nan
    
    # Fill with no extra margin for type 1
    df['Margin Type 1'] = df['Margin'].fillna(0)
    df['Capacity Type 1'] = df['Total'] / (1 - df['Margin Type 1'])
    df['Excess Capacity Type 1'] = (df['Total']/(1-df['Margin Type 1'])) - df['Total']
    
    # Fill with no extra margin for type 2
    weighted_average_margin = ((df['Margin'] * df['capacity']) / df['capacity'].sum()).sum()
    df['Margin Type 2'] = df['Margin'].fillna(weighted_average_margin)
    df['Capacity Type 2'] = df['Total'] / (1 - df['Margin Type 2'])
    df['Excess Capacity Type 2'] = (df['Total']/(1-df['Margin Type 2'])) - df['Total']
    
    
    # creating a break row and a totals row
    row = {'Pipeline Name':'','Key Point':'Total', 'Margin':np.nan, 'Margin Type 1':np.nan, 'Margin Type 2': np.nan} 
    for x in df.columns:
        if x not in row.keys():
            row[x] = df[x].sum()

    row['Margin Type 1'] = row['Excess Capacity Type 1']/ row['Capacity Type 1']
    row['Margin Type 2'] = row['Excess Capacity Type 2']/ row['Capacity Type 2']
    
    empty_row = {x: '' if x in ['Pipeline Name','Key Point'] else np.nan for x in row.keys()}
    df = df.append(pd.DataFrame([empty_row,row]))
    
    # setting the index and renaming the columns
    df = df.set_index(['Pipeline Name','Key Point'])
    df = df.rename(
        columns = {
            'Total':'Total Throughput',
            'capacity': 'Reported Capacity'
        }
    )
    
    # formatting to rounded numbers
    df = df.applymap(lambda x: round(x, 2))
    
    # determine if we want to see the trade type breakdown
    if not show_trade_type:
        df = df[[
            'Total Throughput',
            'Reported Capacity',
            'Margin',
            'Margin Type 1',
            'Capacity Type 1',
            'Excess Capacity Type 1',
            'Margin Type 2',
            'Capacity Type 2',
            'Excess Capacity Type 2'
        ]]
    if return_type == 'table':
        return df.fillna(' ')
    else:
        return df[return_type].loc[('','Total')]