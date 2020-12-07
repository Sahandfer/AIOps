#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import pandas as pd
import numpy as np
import networkx as nx
import glob
from sklearn.cluster import Birch
from sklearn import preprocessing
import numpy as np

metric_step = '5s'
smoothing_window = 12

class MicroRCA():
    def __init__(self, metric_step = '5s', smoothing_window = 12, threshold = 0.03):
        self.metric_step = metric_step
        self.smoothing_window = smoothing_window
        self.threshold = threshold

    def data_processing(self, data):
        data['serviceName'] = data['serviceName'].mask(pd.isnull, data['dsName'])
        # self.data['host_service'] = self.data['cmdb_id'] +':'+ self.data['serviceName']

        # data['callType'] = pd.Categorical(data['callType'], ["OSB", "CSF", "LOCAL", "FlyRemote", "RemoteProcess", "JDBC"])
        # data = data.sort_values(["traceId","startTime"], ignore_index=True)
        data = data.sort_values(["startTime"], ignore_index=True)

        # Get the child's id and elapse time
        elapse_time = {}
        children = {}
        parent_service = {}
        for index, row in data.iterrows():
            if row['pid'] != 'None':
                if row['pid'] in children.keys():
                    children[row['pid']].append(row['id'])
                else:
                    children[row['pid']] = [row['id']]
            elapse_time [row['id']] = float(row['elapsedTime'])
            # If dont need parent node's info in path column just comment this
            parent_service[row['id']] = row['cmdb_id'] + ':' + row['serviceName']

        #Compute for actual elapse_time and edge
        data['actual_time'] = 0.0
        data['path'] = ''
        for index, row in data.iterrows():
            total_child = 0.0

            # If dont need parent node's info in path column just comment this part
            if row['pid'] not in parent_service.keys():
                data.at[index, 'path'] = 'Start-' + row['cmdb_id'] + ':' + row['serviceName']
            else:
                data.at[index, 'path'] = parent_service[row['pid']] + '-' +row['cmdb_id'] + ':'  + row['serviceName']

            if row['id'] not in children.keys():
                data.at[index, 'actual_time'] = row['elapsedTime']
                continue
            for child in children[row['id']]:
                total_child += elapse_time[child]
            data.at[index, 'actual_time'] = row['elapsedTime'] - total_child

        return data


# Create Graph
    def mpg(self, trace_data):
        DG = nx.DiGraph()
        serviceNames = trace_data.host_service.dropna().unique()

        for service in serviceNames:
            pid_options = []
            filtered = trace_data[trace_data.host_service==service]
            pids = filtered.pid.dropna().unique()
            pids = pids[pids!='None']
            for pid in pids:
                selected = trace_data[trace_data.id==pid]
                if not selected.empty:
                    pid_options.append(selected.host_service.iloc[0])
            pid_options = list(set(pid_options))
            pid_options.sort()
            print('destination: %s, sources: %s' %(service,pid_options))
            for source in pid_options:
                DG.add_edge(source, service)   
        
        return DG
    
    ### Anomaly detection function
    def anomaly_detection(self, data):
        services = data.path.unique()
        anomalies = []
        for col in list(services):
            time_vals = df[df["path"]==col]['actual_time']
            anomaly = self.birch_algorithm(time_vals, smoothing_window = self.smoothing_window, threshold = self.threshold)
            if anomaly:
                nodes = col.split('-')
                # if len(nodes) > 1:
                #     host = nodes[1].split(':')[0]
                # else:
                #     host = nodes[0].split(':')[0]
                # print(nodes[1])
                anomalies.append(nodes[1])

        return anomalies

    # # # birch_algorithm (Dont have to call this function)
    ### anomaly_detection function will call this
    def birch_algorithm(self, latency, smoothing_window = 12, threshold = 0.03):
        # put your version here herny
        latency = latency.rolling(window=smoothing_window, min_periods=1).mean()
        x = np.array(latency)
        x = np.where(np.isnan(x), 0, x)
        normalized_x = preprocessing.normalize([x])

        X = normalized_x.reshape(-1,1)

        brc = Birch(branching_factor=50, n_clusters=None, threshold=threshold, compute_labels=True)
        brc.fit(X)
        brc.predict(X)
        labels = brc.labels_
        n_clusters = np.unique(labels).size
        
        if n_clusters > 1:
            return True #True if anomaly
        return False
    
# use your path
path = r'D:\\THU Studies\\Advance Network Management\\Project\\training_data\\2020_05_04\\trace\\unprocessed\\' 
all_files = glob.glob(path + "*.csv")
dfs = []
for filename in all_files:
    df = pd.read_csv(filename, index_col=None)
    dfs.append(df)
df = pd.concat(dfs, axis=0, ignore_index=True)
del dfs

RCA = MicroRCA(smoothing_window = 3, threshold = 0.03)
df = RCA.data_processing(df)
anomalies = RCA.anomaly_detection(df)
print(anomalies)


# path = os.path.dirname(os.path.abspath(__file__))
# data = pd.read_csv(path + '\\training_data\\2020_05_04\\trace\\trace_data_sample.csv', index_col=False)
# data.drop('Unnamed: 0', axis=1, inplace = True)

# DG = mpg(data)
# print('done')

# print(DG.nodes)
# print(DG.edges)





