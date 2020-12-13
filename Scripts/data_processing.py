import pandas as pd
import tqdm

def trace_processing(trace_df):
    elapse_time = {}
    children = {}
    parent_service = {}
    for index, row in tqdm(df.iterrows()):
        if row['pid'] != 'None':
            if row['pid'] in children.keys():
                children[row['pid']].append(row['id'])
            else:
                children[row['pid']] = [row['id']]
        elapse_time [row['id']] = float(row['elapsedTime'])
        parent_service[row['id']] = row['serviceName']

    df['actual_time'] = 0.0
    df['path'] = ''
    for index, row in tqdm(df.iterrows()):
        total_child = 0.0
        if row['pid'] not in parent_service.keys():
            df.at[index, 'path'] = 'Start-' + row['serviceName']
        else:
            df.at[index, 'path'] = parent_service[row['pid']] + '-' + row['serviceName']

        if row['id'] not in children.keys():
            df.at[index, 'actual_time'] = row['elapsedTime']
            continue
        for child in children[row['id']]:
            total_child += elapse_time[child]
        df.at[index, 'actual_time'] = row['elapsedTime'] - total_child