#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import glob
import time
import pandas as pd
import numpy as np
import networkx as nx
from sklearn.cluster import Birch
from sklearn.neighbors import KernelDensity
from sklearn import preprocessing


class MicroRCA():
    def __init__(self, data, metric_step='5s', smoothing_window=12, threshold=0.03):
        self.data = data
        self.metric_step = metric_step
        self.smoothing_window = smoothing_window
        self.threshold = threshold

    def data_processing(self):
        print("Starting data processing...")

        self.data['serviceName'] = self.data['serviceName'].mask(
            pd.isnull, self.data['dsName'])
        # self.data['host_service'] = self.data['cmdb_id'] +':'+ self.data['serviceName']

        # data['callType'] = pd.Categorical(data['callType'], ["OSB", "CSF", "LOCAL", "FlyRemote", "RemoteProcess", "JDBC"])
        # data = data.sort_values(["traceId","startTime"], ignore_index=True)
        self.data = self.data.sort_values(["startTime"], ignore_index=True)

        # Get the child's id and elapse time
        elapse_time = {}
        children = {}
        parent_service = {}
        for index, row in self.data.iterrows():
            if row['pid'] != 'None':
                if row['pid'] in children.keys():
                    children[row['pid']].append(row['id'])
                else:
                    children[row['pid']] = [row['id']]
            elapse_time[row['id']] = float(row['elapsedTime'])
            # If dont need parent node's info in path column just comment this
            parent_service[row['id']] = row['cmdb_id'] + \
                ':' + row['serviceName']

        # Compute for actual elapse_time and edge
        self.data['actual_time'] = 0.0
        self.data['path'] = ''
        for index, row in self.data.iterrows():
            total_child = 0.0

            # If dont need parent node's info in path column just comment this part
            if row['pid'] not in parent_service.keys():
                self.data.at[index, 'path'] = 'Start-' + \
                    row['cmdb_id'] + ':' + row['serviceName']
            else:
                self.data.at[index, 'path'] = parent_service[row['pid']] + \
                    '-' + row['cmdb_id'] + ':' + row['serviceName']

            if row['id'] not in children.keys():
                self.data.at[index, 'actual_time'] = row['elapsedTime']
                continue
            for child in children[row['id']]:
                total_child += elapse_time[child]
            self.data.at[index,
                         'actual_time'] = row['elapsedTime'] - total_child
        
        print("Completed data processing!\n")

    # Anomaly detection function
    def anomaly_detection(self):
        print("Starting anomaly detection...")
        services = self.data.path.unique()
        anomalies = []
        for col in list(services):
            time_vals = self.data[self.data["path"] == col]['actual_time']
            anomaly = self.birch_algorithm(
                time_vals, smoothing_window=self.smoothing_window, threshold=self.threshold)
            if anomaly:
                nodes = col.split('-')
                # if len(nodes) > 1:
                #     host = nodes[1].split(':')[0]
                # else:
                #     host = nodes[0].split(':')[0]
                # print(nodes[1])
                anomalies.append(nodes[1])

        print("Anomaly detection finished with %d anomalies\n" %
              (len(anomalies)))
        return anomalies

    def birch_algorithm(self, latency, smoothing_window=12, threshold=0.03):
        latency = latency.rolling(
            window=smoothing_window, min_periods=1).mean()
        x = np.array(latency)
        x = np.where(np.isnan(x), 0, x)
        normalized_x = preprocessing.normalize([x])

        X = normalized_x.reshape(-1, 1)

        brc = Birch(branching_factor=50, n_clusters=None,
                    threshold=threshold, compute_labels=True)
        brc.fit(X)
        brc.predict(X)
        labels = brc.labels_
        n_clusters = np.unique(labels).size

        if n_clusters > 1:
            return True  # True if anomaly
        return False

    def find_outliers(self, values):
        X = np.reshape(values, (-1, 1))
        kde = KernelDensity(kernel='gaussian', bandwidth=1.0).fit(X)
        yvals = kde.score_samples(X)
        outliers = np.where(yvals < np.percentile(yvals, 1))[0]

        return outliers

    def find_root_causes(self, host_data, anom_hosts):
        print("Starting root cause detection...")

        host_groups = host_data[host_data['cmdb_id'].isin(
            anom_hosts)].groupby('cmdb_id')[['name', 'value']]

        root_causes = []
        for host, item in host_groups:
            df = host_groups.get_group(host)
            name_groups = df.groupby('name')['value'].apply(
                list).reset_index(name='values')
            print("Host %s has %d names" % (host, len(name_groups)))
            for i in range(len(name_groups)):
                row = name_groups.iloc[i]
                name = row['name']
                values = row['values']
                outliers = self.find_outliers(values)
                if (len(outliers)):
                    root_causes.append([host, name])

        print("Root-cause detection finished with %d causes\n" %
              (len(root_causes)))
        return root_causes


if __name__ == "__main__":
    # use your path
    path = 'data/'
    all_files = glob.glob(path + "*.csv")
    dfs = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None)
        dfs.append(df)
    df = pd.concat(dfs, axis=0, ignore_index=True)
    del dfs

    path = 'training_data/2020_05_04/host/'
    host_data = pd.read_csv(path + 'host_data_full.csv', index_col=False)
    host_data.drop('Unnamed: 0', axis=1, inplace=True)

    RCA = MicroRCA(df, smoothing_window=3, threshold=0.03)
    RCA.data_processing()
    anom_hosts = RCA.anomaly_detection()
    
    anoms = []
    for a in anom_hosts:
        item = a.split(':')[0]
        if (item not in anoms):
                anoms.append(item)
    root_causes = RCA.find_root_causes(host_data, anoms)

    print("\nFinal answer:", root_causes)

    # Send root_causes with consumer.py
