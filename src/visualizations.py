import pandas as pd
import os
import datetime
import numpy as np
from plotly.subplots import make_subplots
import plotly.graph_objects as go

def profile_pipeline(df, pipeline = 'Canadian Mainline', grouping_var = 'Trade Type', key_points = []):
    '''
    Create a through put and capacity chart where we can show the distribution by another variable (Trade Type, Product, Key Point)
    Also creates a sub plot to show excess capacity

    Params:
        df: dataframe returned from the get_crude_oil function
        pipeline: str pipeline name
        grouping_var: another df col to groupby
        key_points: select key points on the pipeline to use

    Returns:
        go.Figure Object
    '''
   
    df = df[df['Pipeline Name'] == pipeline]
    
    if key_points:
        df = df[df['Key Point'].isin(key_points)]
    # generate some header columns to save the capacity at each key point
    
    if pipeline != 'Trans Mountain Pipeline':
        header_columns = ['date','Pipeline Name','Key Point']
    else:
        header_columns = ['date','Pipeline Name']
    
    # Pull out the capacity data by selecting some header columns
    cap_data = df[header_columns + ['capacity']].drop_duplicates()
    cap_data = cap_data.groupby(['date','Pipeline Name']).agg({'capacity':'sum'}).reset_index()
    
    # sum up the throughput by trade and product type
    df = df.groupby(['date','Pipeline Name',grouping_var]).agg({'throughput':'sum'}).reset_index()
    
    # pivot out the grouping var (trade types, product, key point)
    groups = df[grouping_var].unique()
    df = df.pivot_table(index = ['date','Pipeline Name'], columns = grouping_var, values = 'throughput').reset_index()
    df['total'] = df[groups].sum(axis = 1)
    
    # add back the capacity and the delta from the throughput
    df = df.merge(cap_data, on = ['date','Pipeline Name'], how = 'left')
    df['delta'] = df['capacity'] - df['total']
    df['excess_cap_flag'] = df['delta'].apply(lambda x: 'red' if x < 0 else 'green')
    
    # Set up the plot structure
    fig = make_subplots(
        rows = 5, 
        cols = 1, 
        specs=[
            [{"rowspan": 3}],
            [None],
            [None],
            [{"rowspan": 2}],
            [None]
        ],
        subplot_titles = [f'{pipeline} Capacity & Utilization 2020',f'{pipeline} Excess Capacity 2020'])
    
    # add the area charts to show the grouping variable distribution
    for x in groups:
        fig.add_trace(
            go.Scatter(
                x = df['date'].values,
                y = df[x],
                name = x,
                fill = 'tonexty',
                stackgroup = 'one',
            )
        )
    
    # Add the capacity line
    fig.add_trace(
        go.Scatter(
            x = df['date'],
            y = df['capacity'],
            name = 'Pipeline Capacity',
            marker = {'color':'black'}
        )
    )
    
    # add the total line
    fig.add_trace(
        go.Scatter(
            x = df['date'],
            y = df['total'],
            name = 'Pipeline Throughput',
            marker = {'color':'green'}
        )
    )
    
    
    fig.add_trace(
        go.Bar(
            x = df['date'],
            y = df['delta'],
            marker = {'color':df['excess_cap_flag']},
            name = 'Excess Capacity'
        ),
        row = 4, col =1
    )
    
    
    fig.update_layout(template = 'plotly_white', height = 700)
#     df = df.value_counts('date')
    return fig


def make_apportionment_charts(pipeline = 'Trans Mountain Pipeline', year = 2020):
    '''
    Reads data and creates an apportionment chart

    Params: 
        pipeline: pipeline name from the get_crude_data df
        year: the year in which the chart should start

    Returns:
        go.Figure object
    '''
    
    df = pd.read_csv('data/apportionment/apportionment-dataset.csv')
    df = df[df.Year >= year]
    df['throughput'] = df['Original Nominations (1000 m3/d)'] * (1 - df['Apportionment Percentage'])
    df['throughput'] = df['throughput'].fillna(df['Accepted Nominations (1000 m3/d)'])
    df['date'] = df.apply(lambda x: datetime.date(x.Year, x.Month, 1), axis = 1)
    
    if pipeline == 'Canadian Mainline':
        df = df[df['Key Point'] == 'system']
    df = df[df['Pipeline Name'] == pipeline]
    
    df = df.dropna(subset = ['Original Nominations (1000 m3/d)', 'throughput'])
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x = df['date'].values,
            y = df['throughput'].values,
            name = 'Throughput (1000 m3/d)'
        )
    )

    fig.add_trace(
        go.Scatter(
            x = df['date'],
            y = df['Original Nominations (1000 m3/d)'].values,
            name = 'Original Nominations (1000 m3/d)',
            fill = 'tonexty'
        )
    )

    fig.update_layout(template = 'plotly_white', title = f'{pipeline} Apportionment')
    return fig