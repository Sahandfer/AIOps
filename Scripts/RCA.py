#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import networkx as nx
from sklearn.neighbors import KernelDensity


class MicroRCA():

    def __init__(self, trace_data, host_data, use_actual_time = True):
        # create a MicroRCA instance
        self.trace_data = trace_data
        self.host_data = host_data
        if use_actual_time:
            self.time_column = 'actual_time'
        else:
            self.time_column = 'elapsedTime'
        print('Using %s.' % self.time_column)
        self.base_graph = nx.DiGraph()


    def run(self):
        # run root cause analysis on the data given
        self.create_graph()
        dodgy_node = self.page_rank()
        print('Pagerank suggests the problematic service is: %s.' % dodgy_node)
        root_causes = self.analyse_host_data(dodgy_node)
        result_to_send_off = []
        for host, kpi, _ in root_causes[:2]:
            result_to_send_off.append([host, kpi])
        print('Thus the result to be sent off to the server is:')
        print(result_to_send_off)


    def create_graph(self):
        # creates weighted graph from the trace data
        edges = list(self.trace_data.path.unique())
        print('Creating graph of %d edges:' % len(edges))
        for edge in edges:
            source, destination = edge.split('-')
            if source != 'Start':
                data_subset = self.trace_data.loc[self.trace_data.path == edge]
                vector_of_actual_time = np.reshape(list(data_subset[self.time_column]), (-1, 1))
                if len(vector_of_actual_time) > 10000:
                    k = len(vector_of_actual_time) // 10000 + 1
                    rnge = np.arange(len(vector_of_actual_time))
                    indices = (rnge % k) == 0
                    vector_of_actual_time = vector_of_actual_time[indices]                
                KDE = KernelDensity(kernel='gaussian', bandwidth=1.0).fit(vector_of_actual_time)
                KDE_score = KDE.score_samples(vector_of_actual_time)
                self.base_graph.add_edge(source, destination, weight=np.mean(KDE_score))
                print('Added edge: %s, ' % edge + 'KDE performed on %d rows' % len(vector_of_actual_time))
        print('Finished creating graph.')

    def page_rank(self):
        # use pagerank to locate the problematic service
        page_rank = nx.pagerank(self.base_graph, alpha=0.85, max_iter=10000)
        page_rank = [(svc, val) for svc, val in dict(sorted(page_rank.items(), key=lambda item: item[1], reverse=True)).items()]
        print('All nodes listed by pagerank:')
        for svc, val in page_rank:
            print('Service name: ' + svc + ', pagerank score: %f' % val)
        return page_rank[0][0]

    def analyse_host_data(self, dodgy_node):
        # given a problematic service, look for the root cause in the service's host data.
        dodgy_hosts = self.trace_data.loc[self.trace_data.serviceName == dodgy_node].cmdb_id.unique()
        host_groups = self.host_data[self.host_data['cmdb_id'].isin(dodgy_hosts)].groupby('cmdb_id')[['name', 'value']]

        root_causes = []
        for host, _ in host_groups:
            root_causes_for_host = []
            df = host_groups.get_group(host)
            name_groups = df.groupby('name')['value'].apply(list).reset_index(name='values')
            for i in range(len(name_groups)):
                row = name_groups.iloc[i]
                name = row['name']
                values = row['values']
                outliers, average_KDE_score = self.find_outliers(values)
                if (len(outliers)):
                    root_causes_for_host.append((name, average_KDE_score))

            KPI_name, KDE_score = min(root_causes_for_host,key=lambda item:item[1])
            root_causes.append((host, KPI_name, KDE_score))
        
        root_causes.sort(key=lambda tripple: tripple[2])
        print('Possible route causes:')
        for host, KPI_name, KDE_score in root_causes:
            print('Host: ' + host + ', KPI name: ' + KPI_name + ', KDE score: %f' % KDE_score)
        return root_causes

    def find_outliers(self, values):
        # flag if a KPI is exhibiting anomalous behaviour
        X = np.reshape(values, (-1, 1))
        KDE = KernelDensity(kernel='gaussian', bandwidth=1.0).fit(X)
        KDE_scores = KDE.score_samples(X)
        outliers = np.where(KDE_scores < np.percentile(KDE_scores, 1))[0]
        return outliers, np.mean(KDE_scores)


# put your path here
path = r'C:\\Users\\spkgy\\OneDrive\\Documents\\Tsinghua\\Advanced Network Management\\Group Project\\'

# adjust file names appropriately
trace = pd.read_csv(path + 'processed_data.csv')
trace = trace.sort_values(by=['startTime'])
trace = trace[trace.startTime < trace.startTime[0]+600000]

host = pd.read_csv(path + 'full_kpi_data.csv')
host = host.sort_values(by=['timestamp'])
host = host[host.timestamp < trace.startTime[0]+700000]

# the third argument is whether or not to use 'actual_time' or 'elapsedTime'. True means use 'actual_time'
RCA = MicroRCA(trace, host, False)
RCA.run()
