#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Sam, Sami, Henry
"""

import os
import time
import pandas as pd
import numpy as np
import networkx as nx
from sklearn.cluster import Birch
from sklearn import preprocessing


class MicroRCA():
    def __init__(self, metric_step='5s', smoothing_window=12):
        self.get_data()
        self.metric_step = metric_step
        self.smoothing_window = smoothing_window

        DG, df = self.attributed_graph()
        self.graph = DG
        self.df = df

    # Step 1: Data collection
    def get_data(self):
        print("Loading dataset...")
        
        # path = 'training_data/2020_05_04/trace/'
        path = 'sample/'
        data = pd.read_csv(path + 'trace_data_sample.csv', index_col=False)
        data.drop('Unnamed: 0', axis=1, inplace=True)
        self.trace_data = data
        print("--- Loaded trace data!")
        
        path = 'training_data/2020_05_04/host/'
        data = pd.read_csv(path + 'full_kpi_data.csv', index_col=False)
        data.drop('Unnamed: 0', axis=1, inplace=True)
        self.host_data = data
        print("--- Loaded host data!")
        
        print("Data successfully loaded!\n")


    # TODO fix birch to find anomalies, currently it returns []
    # Step 2: Detect anomalies using BIRCH

    def birch_anomaly_detection(self, threshold):
        for svc, latency in self.trace_data.iteritems():
            if svc == 'elapsedTime':
                latency = latency.rolling(window=1, min_periods=1).mean()
                x = np.array(latency)
                x = np.where(np.isnan(x), 0, x)
                normalized_x = preprocessing.normalize([x])

                X = normalized_x.reshape(-1, 1)
                brc = Birch(branching_factor=10,
                            threshold=threshold, compute_labels=True)
                brc.fit(X)
                brc.predict(X)

                labels = brc.labels_
                n_clusters = np.unique(labels).size
                return n_clusters


    # TODO: fix graph construction
    # Step 3: Create the attribute graph of sources and hosts

    def attributed_graph(self):
        DG = nx.DiGraph()
        df = pd.DataFrame(columns=['source', 'destination'])
        print('started loop')

        serviceNames = self.data.serviceName.dropna().unique()

        for service in (serviceNames):
            print(service)
            selected = self.data[self.data.serviceName == service]
            trace = selected.iloc[0]
            destination = trace['serviceName']
            assert destination == service

            if trace['pid'] != 'None':
                _temp = True
                i = 0
                filtered = ''
                while _temp:
                    trace = selected.iloc[i]
                    i += 1
                    filtered = self.data[self.data['id'] == trace['pid']]
                    _temp = filtered.empty
                parent = filtered.iloc[0]
                if parent['callType'] == 'JDBC':
                    source = filtered.dsName.iloc[0]
                else:
                    source = filtered.serviceName.iloc[0]
                print('The source for service %s is %s' % (destination, source))
                df = df.append(
                    {'source': source, 'destination': destination}, ignore_index=True)
                DG.add_edge(source, destination)
                DG.nodes[source]['type'] = 'service'

            host = trace['cmdb_id']
            print('The host for service %s is %s' % (service, host))
            DG.add_edge(destination, host)
            DG.nodes[destination]['type'] = 'service'
            DG.nodes[host]['type'] = 'host'
            df = df.append(
                {'source': destination, 'destination': host}, ignore_index=True)
        
        return (DG, df)
        # for service in dsNames:
        #     print('service ', service)
        #     selected = self.data[self.data.dsName == service]
        #     trace = selected.iloc[0]
        #     destination = trace['dsName']
        #     assert destination == service

        #     if trace['pid'] != 'None':
        #         _temp = True
        #         i = 0
        #         filtered = ''
        #         while _temp:
        #             trace = selected.iloc[i]
        #             i += 1
        #             filtered = self.data[self.data['id'] == trace['pid']]
        #             _temp = filtered.empty
        #         parent = filtered.iloc[0]
        #         if parent['callType'] == 'JDBC':
        #             source = filtered.dsName.iloc[0]
        #         else:
        #             source = filtered.serviceName.iloc[0]
        #         print('The source for service %s is %s' % (destination, source))
        #         df = df.append(
        #             {'source': source, 'destination': destination}, ignore_index=True)
        #         DG.add_edge(source, destination)
        #         DG.nodes[source]['type'] = 'service'

        #     host = trace['cmdb_id']
        #     print('The host for service %s is %s' % (service, host))
        #     DG.add_edge(destination, host)
        #     DG.nodes[destination]['type'] = 'service'
        #     DG.nodes[host]['type'] = 'host'
        #     df = df.append(
        #         {'source': destination, 'destination': host}, ignore_index=True)

        


if __name__ == '__main__':
    m_rca = MicroRCA()