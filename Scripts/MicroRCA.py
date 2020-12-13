#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import numpy as np
import pandas as pd
import networkx as nx
from sklearn import preprocessing
from sklearn.cluster import Birch
from sklearn.neighbors import KernelDensity



class MicroRCA():
    def __init__(self, trace_data, host_data, alpha = 0.55):
        self.trace_data = trace_data
        self.host_data = host_data
        self.alpha = alpha
        self.base_graph = nx.DiGraph()
        self.anomalous_subgraph = nx.DiGraph()
        self.edges = []
        self.anomalous_edges  = {}
        self.personalization = {}
        self.localized_kpis = {}


    def run(self):
        print('Started building graph...')
        start_time = time.time()
        self.build_base_graph()
        print('Finished building graph in %f seconds.' % (time.time() - start_time))

        print('Started finding anomalous edges...')
        start_time = time.time()
        self.find_anomalous_edges()
        print('Finished finding anomalous edges in %f seconds.' % (time.time() - start_time))

        print('Started extracting anomalous subgraph...')
        start_time = time.time()
        self.extract_anomalous_subgraph()
        print('Finished extracting anomalous subgraph in %f seconds.' % (time.time() - start_time))

        print('Started finding page rank scores...')
        start_time = time.time()
        output = self.page_rank()
        print('Finished finding page rank scores in %f seconds.' % (time.time() - start_time))

        return output


    def build_base_graph(self):
        self.edges = list(self.trace_data.path.unique())
        for edge in self.edges:
            source, destination = edge.split('-')
            if source != 'Start':
                self.base_graph.add_edge(source,destination)
                self.base_graph.nodes[source]['type'] = 'service'
                self.base_graph.nodes[destination]['type'] = 'service'
                source_hosts = list(self.trace_data[self.trace_data.serviceName==source]['cmdb_id'].unique())
                destination_hosts = list(self.trace_data[self.trace_data.serviceName==destination]['cmdb_id'].unique())
                for source_host in source_hosts:
                    self.base_graph.add_edge(source, source_host)
                    self.base_graph.nodes[source_host]['type'] = 'host'
                for destination_host in destination_hosts:
                    self.base_graph.add_edge(destination, destination_host)
                    self.base_graph.nodes[destination_host]['type'] = 'host'

    
    def find_anomalous_edges(self):
        for edge in self.edges:
            elapsed_time = np.array(list(self.trace_data[self.trace_data.path == edge]['elapsedTime']))
            normalized_time = preprocessing.normalize([elapsed_time]).reshape(-1,1)
            birch = Birch(branching_factor=50, n_clusters=None, threshold=0.001, compute_labels=True)
            birch.fit(normalized_time)
            birch.predict(normalized_time)
            labels = birch.labels_
            if np.unique(labels).size > 1:
                self.anomalous_edges[edge.split('-')[1]] = edge


    def extract_anomalous_subgraph(self):
        for node in self.anomalous_edges.keys():
            for source, destination, data in self.base_graph.in_edges(node, data=True):
                edge = (source + '-' + destination)
                if edge in self.anomalous_edges:
                    data = self.alpha
                else:
                    anomalous_data = pd.Series(list(self.trace_data.loc[self.trace_data.path == self.anomalous_edges[destination]]['elapsedTime']))
                    normal_data = pd.Series(list(self.trace_data.loc[self.trace_data.path == edge]['elapsedTime']))
                    data = 0
                    if len(set(anomalous_data))>1 and len(set(normal_data))>1:
                        data = anomalous_data.corr(normal_data)
                    if pd.isna(data):
                        data=0

                data = round(data, 3)
                self.anomalous_subgraph.add_edge(source, destination, weight=data)
                self.anomalous_subgraph.nodes[source]['type'] = self.base_graph.nodes[source]['type']
                self.anomalous_subgraph.nodes[destination]['type'] = self.base_graph.nodes[destination]['type']

            for source, destination, data in self.base_graph.out_edges(node, data=True):
                edge = (source + '-' + destination)
                if edge in self.anomalous_edges:
                    data = self.alpha
                else:
                    if self.base_graph.nodes[destination]['type'] == 'host':
                        data, KPI = self.get_weight(source, destination)
                    else:
                        anomalous_data = pd.Series(list(self.trace_data.loc[self.trace_data.path == self.anomalous_edges[source]]['elapsedTime']))
                        normal_data = pd.Series(list(self.trace_data.loc[self.trace_data.path == edge]['elapsedTime']))
                        data = 0
                        if len(set(anomalous_data))>1 and len(set(normal_data))>1:
                            data = anomalous_data.corr(normal_data)
                        if pd.isna(data):
                            data=0

                data = round(data, 3)
                self.anomalous_subgraph.add_edge(source, destination, weight=data)
                self.anomalous_subgraph.nodes[source]['type'] = self.base_graph.nodes[source]['type']
                self.anomalous_subgraph.nodes[destination]['type'] = self.base_graph.nodes[destination]['type']
        
        for node in self.anomalous_edges.keys():
            data, host, KPI = self.get_personalization(node)
            self.personalization[node] = data/self.anomalous_subgraph.degree(node)
            self.localized_kpis[node] = (host, KPI)

        self.anomalous_subgraph = self.anomalous_subgraph.reverse(copy = True)

        # sdd = list(self.anomalous_subgraph.edges(data=True))
        # for source, destination, data in sdd:
        #     if self.anomalous_subgraph.nodes[source]['type'] == 'host':
        #         self.anomalous_subgraph.remove_edge(source,destination)
        #         self.anomalous_subgraph.add_edge(destination,source,weight=data['weight'])


    def page_rank(self):
        anomaly_scores = nx.pagerank(self.anomalous_subgraph, alpha=0.85, personalization=self.personalization, max_iter=10000)
        anomaly_scores = sorted(anomaly_scores.items(), key=lambda x: x[1], reverse=True)
        print('The services ordered by page rank are:')
        print(anomaly_scores)
        print('The most anomalous host and KPI of each anomalous node:')
        for host, _ in anomaly_scores:
            if host in self.localized_kpis.keys():
                print(self.localized_kpis[host])
        return self.localized_kpis[anomaly_scores[0][0]]

        
    def get_weight(self, service, host):
            in_edges_weight_avg = 0.0
            num = 0
            max_corr = 0.01
            metric = -1
            for _, _, data in self.anomalous_subgraph.in_edges(service, data=True):
                in_edges_weight_avg += data['weight']
                num += 1            
            if num > 0:
                in_edges_weight_avg  = in_edges_weight_avg / num            

            host_data_subset = self.host_data[self.host_data['cmdb_id']==host][['name', 'value']]

            for KPI, values in host_data_subset.groupby('name')['value']:
                anomalous_data = pd.Series(list(self.trace_data.loc[self.trace_data.path == self.anomalous_edges[service]]['elapsedTime']))
                values = pd.Series(list(values))
                correlation = 0
                if len(set(anomalous_data))>1 and len(set(values))>1:
                    correlation = abs(anomalous_data.corr(values))
                if pd.isna(correlation):
                    correlation = 0
                if correlation > max_corr:
                    max_corr = correlation
                    metric = KPI

            data = in_edges_weight_avg * max_corr
            return data, metric

    
    def get_personalization(self, service):
            weight_average = 0.0
            num = 0
            max_corr = 0.01
            metric = -1   
            metric_host = -1     
            for _, _, data in self.anomalous_subgraph.in_edges(service, data=True):
                    weight_average += data['weight']
                    num += 1   

            for _, destination, data in self.anomalous_subgraph.out_edges(service, data=True):
                        if self.anomalous_subgraph.nodes[destination]['type'] == 'service':
                            num += 1
                            weight_average += data['weight']

            hosts = self.trace_data.loc[self.trace_data.serviceName == service].cmdb_id.unique()
            host_groups = self.host_data[self.host_data['cmdb_id'].isin(hosts)].groupby('cmdb_id')[['name', 'value']]

            for host, host_data_subset in host_groups:
                for KPI, values in host_data_subset.groupby('name')['value']:
                    anomalous_data = pd.Series(list(self.trace_data.loc[(self.trace_data.path == self.anomalous_edges[service]) & (self.trace_data.cmdb_id == host)]['elapsedTime']))
                    values = pd.Series(list(values))
                    correlation = 0
                    if len(set(anomalous_data))>1 and len(set(values))>1:
                        correlation = abs(anomalous_data.corr(values))
                    if pd.isna(correlation):
                        correlation = 0
                    if correlation > max_corr:
                        metric_host = host
                        max_corr = correlation
                        metric = KPI
        
            data = weight_average * max_corr
            return data, metric_host, metric


path = r'C:\\Users\\spkgy\\OneDrive\\Documents\\Tsinghua\\Advanced Network Management\\Group Project\\'

t = pd.read_csv(path+'trace_untouched_5_26.csv')
h = pd.read_csv(path+'kpi_data_526.csv')

detector = MicroRCA(t, h)
detector.run()