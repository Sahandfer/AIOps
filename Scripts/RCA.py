#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import networkx as nx
from sklearn.neighbors import KernelDensity
from sklearn.cluster import Birch
from sklearn import preprocessing
import matplotlib.pyplot as plt
import math


class RCA():

    def __init__(self, trace_data, host_data, use_actual_time = True, take_minute_averages_of_trace_data = False, \
        find_root_cause_with_KDE = False, find_using_corr_method = True):
        # create a RCA instance
        self.trace_data = trace_data
        self.host_data = host_data
        if use_actual_time:
            self.time_column = 'actual_time'
        else:
            self.time_column = 'elapsedTime'
        print('Using %s.' % self.time_column)
        self.base_graph = nx.DiGraph()

        self.dictionary_of_times = {}
        self.edges = list(self.trace_data.path.unique())
        if take_minute_averages_of_trace_data:
            print('Taking 1 minute averages of the trace data.')
            for edge in self.edges:
                data_subset = self.trace_data.loc[self.trace_data.path == edge].copy()
                data_subset['time_group'] = data_subset.startTime//60000
                averaged_data = data_subset.groupby('time_group')[self.time_column].mean()
                self.dictionary_of_times[edge] = list(averaged_data)
        else:
            print('Using the full trace data, no averages taken.')
            for edge in self.edges:
                data_subset = self.trace_data.loc[self.trace_data.path == edge]
                self.dictionary_of_times[edge] = list(data_subset[self.time_column])

        self.find_using_corr_method = find_using_corr_method

        self.find_root_cause_with_KDE = find_root_cause_with_KDE
        if self.find_root_cause_with_KDE:
            print('Root cause localisation will be performed using KDE')
        else:            
            print('Root cause localisation will be performed using Birch')


    def run(self):
        # run root cause analysis on the data given
        self.create_graph()
        # self.personalize_graph()
        self.create_personalization()
        dodgy_node = self.page_rank()
        print('The ranks suggest the problematic service is: %s.' % dodgy_node)
        root_causes = self.analyse_host_data(dodgy_node)
        
        # for nodes in self.base_graph.nodes:
        #     max_corr = self.analyse_host_data(node)

        #     for _, _, data in self.base_graph.out_edges(node):
        #         #calcualte average...

        #     self.base_graph.nodes[node]['personalisation'] = max_corr * ____

        # result_to_send_off = []
        # for host, kpi, _ in root_causes[:2]:
        #     result_to_send_off.append([host, kpi])
        print('Thus the result to be sent off to the server is:')
        print(result_to_send_off)
        return result_to_send_off
        # for node in dodgy_nodes:
        #     root_causes = self.analyse_host_data(node)
        #     result_to_send_off = []
        #     for host, kpi, _ in root_causes[:2]:
        #         result_to_send_off.append([host, kpi])
        #     print('Thus the result to be sent off to the server is:')
        #     print(result_to_send_off)


    def create_graph(self):
        # creates weighted graph from the trace data
        print('Creating graph of %d edges:' % len(self.edges))
        for edge in self.edges:
            source, destination = edge.split('-')
            if source != 'Start':
                vector_of_time = self.dictionary_of_times[edge]
                reshaped_vector_of_time = np.reshape(vector_of_time, (-1,1))
                if len(reshaped_vector_of_time) > 5000:
                    k = len(reshaped_vector_of_time) // 5000 + 1
                    rnge = np.arange(len(reshaped_vector_of_time))
                    indices = (rnge % k) == 0
                    reshaped_vector_of_time = reshaped_vector_of_time[indices]                
                KDE = KernelDensity(kernel='gaussian', bandwidth=1.0).fit(reshaped_vector_of_time)
                KDE_scores = KDE.score_samples(reshaped_vector_of_time)
                mean_of_KDE_scores = - np.mean(KDE_scores)

                normalized_vector_of_time = preprocessing.normalize([vector_of_time]).reshape(-1,1)
                birch = Birch(n_clusters=None, threshold=0.1, compute_labels=True)
                birch.fit(normalized_vector_of_time)
                birch.predict(normalized_vector_of_time)
                labels = birch.labels_
                birch_clustering_score = 100 * len(labels[np.where(labels!=0)])/len(labels)

                total_weight = mean_of_KDE_scores * birch_clustering_score + mean_of_KDE_scores + birch_clustering_score

                self.base_graph.add_edge(source, destination, weight=total_weight)
                print('Added edge: %s with weight %f, ' % (edge, total_weight) + 'KDE performed on %d rows' % len(reshaped_vector_of_time))
        print('Finished creating graph.')


    def personalize_graph(self):
        self.personalization = {}
        for node in self.base_graph.nodes:
            self.personalization[node] = 1
        # print(self.personalization)
                
        # positions = {}
        # positions['os_022'] = (1,4)
        # positions['os_021'] = (4,4)
        # positions['docker_002'] = (0.5,3)
        # positions['docker_001'] = (1.5,3)
        # positions['docker_003'] = (3.5,3)
        # positions['docker_004'] = (4.5,3)
        # positions['docker_007'] = (0,2)
        # positions['docker_008'] = (1,2)
        # positions['db_007'] = (2,2)
        # positions['db_009'] = (3,2)
        # positions['docker_006'] = (4,2)
        # positions['docker_005'] = (5,2)
        # positions['db_003'] = (2.5,1)
        # nx.draw_networkx(self.base_graph, positions, node_size = 5500, node_color = '#00BFFF')
        # plt.show()


    def page_rank(self):
<<<<<<< HEAD
        self.base_graph=self.base_graph.reverse(copy=True)
=======
>>>>>>> 9099032c0903ec475083d5f0fcb0c0bca08c7f91
        page_rank = nx.pagerank(self.base_graph, alpha=0.85, personalization = self.personalization ,max_iter=10000)
        page_rank = [(svc, val) for svc, val in dict(sorted(page_rank.items(), key=lambda item: item[1], reverse=True)).items()]
        # page_rank = []
        # for node in self.base_graph.nodes:
<<<<<<< HEAD
            # weight = 0
            # for _, _, d in self.base_graph.in_edges(node, data=True):
                # weight += d['weight']
            # val = weight
            # page_rank.append((node, val))
=======
        #     weight = 0
        #     for _, _, d in self.base_graph.in_edges(node, data=True):
        #         weight += d['weight']
        #     val = weight
        #     page_rank.append((node, val))
>>>>>>> 9099032c0903ec475083d5f0fcb0c0bca08c7f91
        page_rank.sort(key=lambda tripple: tripple[1], reverse = True)
        print('All nodes listed by their rank:')
        for node, val in page_rank:
            print('Service name: ' + node + ', score: %f' % val)
        return page_rank[0][0]



    def analyse_host_data(self, dodgy_node):
        if self.find_using_corr_method:
            return [dodgy_node, self.base_graph[dodgy_node]['type']]
        # given a problematic service, look for the root cause in the service's host data.
        # dodgy_hosts = self.trace_data.loc[self.trace_data.serviceName == dodgy_node].cmdb_id.unique()
        # host_groups = self.host_data[self.host_data['cmdb_id'].isin(dodgy_hosts)].groupby('cmdb_id')[['name', 'value']]

        host_groups = self.host_data[self.host_data['cmdb_id']==dodgy_node].groupby('cmdb_id')[['name', 'value']]

        root_causes = []
        for host, _ in host_groups:
            root_causes_for_host = []
            df = host_groups.get_group(host)
            name_groups = df.groupby('name')['value'].apply(list).reset_index(name='values')
            for i in range(len(name_groups)):
                row = name_groups.iloc[i]
                name = row['name']
                values = row['values']
                if len(set(values))> 1:
                    outliers, score = self.find_outliers(values)
                    if outliers:
                        root_causes_for_host.append((name, score))

            if len(root_causes_for_host)>0:
                KPI_name, score = max(root_causes_for_host,key=lambda item:item[1])
                root_causes.append((host, KPI_name, score))
        
        root_causes.sort(key=lambda tripple: tripple[2], reverse = True)
        print('Possible route causes are listed below:')
        for host, KPI_name, KDE_score in root_causes:
            print('Host: ' + host + ', KPI name: ' + KPI_name + ', Score: %f' % KDE_score)
        return root_causes

    def find_outliers(self, values, dodgy_node='hello'):
        # flag if a KPI is exhibiting anomalous behaviour
        if self.find_root_cause_with_KDE:
            X = np.reshape(values, (-1, 1))
            KDE = KernelDensity(kernel='gaussian', bandwidth=1.0).fit(X)
            KDE_scores = KDE.score_samples(X)
            outliers = np.where(KDE_scores < np.percentile(KDE_scores, 1))[0]
            return (len(outliers) > 0), - np.mean(KDE_scores)
        else:
            normalized_values = preprocessing.normalize([values]).reshape(-1,1)
            birch = Birch(n_clusters=None, threshold=0.06, compute_labels=True)
            birch.fit(normalized_values)
            birch.predict(normalized_values)
            labels = birch.labels_
            birch_clustering_score = len(labels[np.where(labels!=0)])/len(labels)
            return (birch_clustering_score > 0), birch_clustering_score

    def node_weight(self, node):
        anomaly_graph = self.base_graph
        trace_df = self.trace_data
        host_df = self.host_data

        #trace_df = anomalous trace edge (pser-ser)
        #host_df = host_data with same cmdb id as edge 1
        svc = node
        kpi_df = host_df[host_df.cmdb_id==svc]
        # for host, _ in host_groups:
        root_causes_for_host = []
        trace_df = trace_df[trace_df['serviceName']==svc]
        # anomalous_trace = anomalous_trace['actual_time']
        trace_groups = trace_df.groupby('callType')['actual_time']
        call_groups = trace_df.callType.unique()
        # print(anomalous_trace)

        # anomalous_trace['startTime'] = pd.to_datetime(anomalous_trace.startTime, unit='ms')
        # anomalous_trace = anomalous_trace.set_index(pd.DatetimeIndex(anomalous_trace['startTime'])+ pd.Timedelta('08:00:00'))
        # anomalous_trace['actual_time'].resample('1Min').mean()
        # print(anomalous_trace)

        kpi_groups = kpi_df.groupby('name')['value']
        name_groups = kpi_df.name.unique()

        max_corr = 0.01
        metric = 'Nothing correlated'
        for call in call_groups:
            trace = trace_groups.get_group(call)
            for name in name_groups:
                hdf = kpi_groups.get_group(name)
                # print('hdf', str(name), hdf.to_numpy().flatten())
                if len(set(hdf.to_numpy().flatten()))> 1:
                    # print(anomalous_trace.to_numpy().flatten())
                    # print(hdf.to_numpy().flatten())
                    tmp = abs(trace.corr(hdf))
                    # print(tmp)
                    if math.isnan(tmp):
                        tmp = 0
                    elif tmp > max_corr:
                        max_corr = tmp
                        metric = str(name)


        edges_weight_avg = 0.0
        num = 0
        for u, v, data in anomaly_graph.in_edges(svc, data=True):
            num = num + 1
            edges_weight_avg = edges_weight_avg + data['weight']

        for u, v, data in anomaly_graph.out_edges(svc, data=True):
            # if anomaly_graph.nodes[v]['type'] == 'service':
            num = num + 1
            edges_weight_avg = edges_weight_avg + data['weight']

        edges_weight_avg  = edges_weight_avg / num

        personalization = edges_weight_avg * max_corr

        return personalization, metric

    def create_personalization(self):
        personalization = {}
        for node in self.base_graph.nodes():
            max_corr, component = self.node_weight(node)
            personalization[node] = max_corr / self.base_graph.degree(node)
            self.base_graph.nodes[node]['type'] = str(component)
            print(personalization[node])
        self.personalization = personalization


# put your path here
# path = r'C:\\Users\\spkgy\\OneDrive\\Documents\\Tsinghua\\Advanced Network Management\\Group Project\\'
path = r'D:\\THU Studies\\Advance Network Management\\Project\\Anomaly-detection\\local_data\\'
        # if self.find_using_corr_method:
        #     subset_data = self.trace_data.loc[self.trace_data.serviceName == dodgy_node].elapsedTime
        #     x = subset_data.corrwith(values)
        #     return x



# put your path here
# path = r'C:\\Users\\spkgy\\OneDrive\\Documents\\Tsinghua\\Advanced Network Management\\Group Project\\'

# adjust file names appropriately
trace = pd.read_csv(path + 'trace_5_26.csv')
trace = trace.sort_values(by=['startTime'])
# trace = trace[trace.startTime < trace.startTime[0]+1260000]

host = pd.read_csv(path + 'kpi_data_526.csv')
host = host.sort_values(by=['timestamp'])
# host = host[host.timestamp < host.timestamp[0]+1260000]

# # the third argument is whether or not to use 'actual_time' or 'elapsedTime'. True means use 'actual_time'
RCA = RCA(trace, host, use_actual_time = True, take_minute_averages_of_trace_data = False, \
        find_root_cause_with_KDE = True, find_using_corr_method = True)
RCA.run()
# print(RCA.node_weight(node = 'docker_002'))
