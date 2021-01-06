#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import numpy as np
import pandas as pd
import networkx as nx
from termcolor import colored
from sklearn import preprocessing
from sklearn.cluster import Birch
from sklearn.neighbors import KernelDensity



class MicroRCA():
    def __init__(self, trace_data, host_data, alpha = 0.55, take_minute_averages_of_trace_data = True, division_milliseconds = 60000):
        self.trace_data = trace_data
        self.host_data = host_data
        self.alpha = alpha
        self.base_graph = nx.DiGraph()
        self.anomalous_subgraph = nx.DiGraph()
        self.edges = list(self.trace_data.path.unique())
        self.anomalous_edges  = {}
        self.personalization = {}
        self.localized_kpis = {}
        self.take_minute_averages_of_trace_data = take_minute_averages_of_trace_data

        if take_minute_averages_of_trace_data:
            print('Taking %dms averages of the trace data.' % division_milliseconds)
            averaged_datasets = []
            for edge in self.edges:
                data_subset = self.trace_data.loc[self.trace_data.path == edge].copy()
                data_subset['time_group'] = data_subset.startTime//division_milliseconds
                averaged_data = data_subset.groupby(['path', 'serviceName', 'cmdb_id', 'time_group'])['elapsedTime'].mean().reset_index()
                averaged_datasets.append(averaged_data)            
            self.trace_data = pd.concat(averaged_datasets, ignore_index = True).sort_values(by = ['time_group'])
        else:
            print('Using the full trace data, no averages taken.')
            for edge in self.edges:
                data_subset = self.trace_data.loc[self.trace_data.path == edge]


    def run(self):
        print('Running RCA on %d trace data rows and %d host data rows' % (len(self.trace_data), len(self.host_data)))
        overall_start_time = time.time()

        print('Started building graph...')
        start_time = time.time()
        self.build_base_graph()
        print('Finished building graph in ' + colored('%f','cyan') % (time.time() - start_time) + ' seconds.')

        print('Started finding anomalous edges...')
        start_time = time.time()
        self.find_anomalous_edges()
        print('Finished finding anomalous edges in ' + colored('%f','cyan') % (time.time() - start_time) + ' seconds.')

        print('Started extracting anomalous subgraph...')
        start_time = time.time()
        self.extract_anomalous_subgraph()
        print('Finished extracting anomalous subgraph in ' + colored('%f','cyan') % (time.time() - start_time) + ' seconds.')

        print('Started finding pagerank scores...')
        start_time = time.time()
        output = self.page_rank()
        print('Finished finding pagerank scores in ' + colored('%f','cyan') % (time.time() - start_time) + ' seconds.')
        print('The output to send to the server is: '+ colored(str(output), 'magenta'))

        print('RCA finished in ' + colored('%f','cyan') % (time.time() - overall_start_time) + ' seconds.')
        return output


    def build_base_graph(self):
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
            if self.take_minute_averages_of_trace_data:
                birch = Birch(branching_factor=50, n_clusters=None, threshold=0.05, compute_labels=True)
            else:
                birch = Birch(branching_factor=50, n_clusters=None, threshold=0.001, compute_labels=True)
            birch.fit_predict(normalized_time)
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
                        data, _ = self.get_weight(source, destination)
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
            data, metrics = self.get_personalization(node)
            self.personalization[node] = data/self.anomalous_subgraph.degree(node)
            self.localized_kpis[node] = metrics

        self.anomalous_subgraph = self.anomalous_subgraph.reverse(copy = True)

        # sdd = list(self.anomalous_subgraph.edges(data=True))
        # for source, destination, data in sdd:
        #     if self.anomalous_subgraph.nodes[source]['type'] == 'host':
        #         self.anomalous_subgraph.remove_edge(source,destination)
        #         self.anomalous_subgraph.add_edge(destination,source,weight=data['weight'])


    def page_rank(self):
        try:
            anomaly_scores = nx.pagerank(self.anomalous_subgraph, alpha=0.85, personalization=self.personalization, max_iter=10000)
            anomaly_scores = sorted(anomaly_scores.items(), key=lambda x: x[1], reverse=True)
        except:
            print(colored('Pagerank did not converge', 'red'))
            return []

        if len(anomaly_scores)>0:
            print('The services with pagerank score exceeding 0 are:')
            col_width = max(len(str(word)) for row in anomaly_scores for word in row) 
            for pair in anomaly_scores:
                if pair[1]>0:
                    print("".join(str(word).ljust(col_width) for word in pair))
            return [[host, KPI] for host, KPI, _ in self.localized_kpis[anomaly_scores[0][0]]]
        else:
            print(colored('NO ANOMALIES DETECTED', 'red'))
            return []

        
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
            metrics = []    
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
                        normalized_time = preprocessing.normalize([np.array(values)]).reshape(-1,1)
                        birch = Birch(branching_factor=50, n_clusters=None, threshold=0.005, compute_labels=True)
                        birch.fit_predict(normalized_time)
                        labels = birch.labels_
                        coefficient = int(np.unique(labels).size > 1)
                        correlation = coefficient * correlation
                    if pd.isna(correlation):
                        correlation = 0
                    if correlation > max_corr:
                        metrics.append((host, KPI, correlation))
                        max_corr = correlation
        
            data = weight_average * max_corr
            metrics.sort(key = lambda tup: tup[2], reverse = True )
            if len(metrics) > 1:
                if metrics[1][2]/metrics[0][2] > 0.9:
                    return data, metrics[:2]
                else:
                    return data, metrics[:1]
            else:
                return data, metrics


path = r'C:\\Users\\spkgy\\OneDrive\\Documents\\Tsinghua\\Advanced Network Management\\Group Project\\'

t = pd.read_csv(path+'trace_untouched_5_26.csv')
h = pd.read_csv(path+'kpi_data_526.csv')

detector = MicroRCA(t, h, take_minute_averages_of_trace_data = True)
detector.run()