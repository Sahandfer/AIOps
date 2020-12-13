#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Example for RCAing like a B0$$
'''
import requests
import json

from kafka import KafkaConsumer

import time
import numpy as np
import pandas as pd
import networkx as nx
from sklearn import preprocessing
from sklearn.cluster import Birch
from sklearn.neighbors import KernelDensity

class MicroRCA():
    def __init__(self, esb_data, trace_data = None, host_data = None, alpha = 0.55):
        self.esb_data = esb_data
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
        output = [self.page_rank()]
        print('Finished finding page rank scores in %f seconds.' % (time.time() - start_time))

        print(type(output))
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
            self.localized_kpis[node] = [host, KPI]

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
        
    def update_esb_data(self, esb_data):
        self.esb_data = esb_data

    def update_trace_data(self, trace_data):
        self.trace_data = trace_data
        self.trace_processing()

    def update_host_data(self, host_data):
        self.host_data = host_data

    def birch(self, values):  # values should be a list
        X = np.reshape(values, (-1, 1))
        brc = Birch(n_clusters=None)
        brc.fit(X)

        return brc.predict(X)

    def kde(self, values):
        X = np.reshape(values, (-1, 1))
        KDE = KernelDensity(kernel='gaussian', bandwidth=1.0).fit(X)
        KDE_scores = KDE.score_samples(X)
        outliers = np.where(KDE_scores < np.percentile(KDE_scores, 1))[0]

        return (len(outliers) > 0), - np.mean(KDE_scores)

    def analyze_esb(self, esb_dict):
        esb_tmp = self.esb_data.append(esb_dict, ignore_index=True)
        values = esb_tmp['avg_time'].tolist()
        print(values)
        birch_labels_time = self.birch(values)
        # birch_labels_rate = self.birch(self.esb_data['avg_time'])
        for label in birch_labels_time:
            if (label != 0):
                print("Found esb_anomaly in avg_time")
                return True

        values = esb_tmp['succee_rate'].tolist()
        print(values)
        birch_labels_time = self.birch(values)
        for label in birch_labels_time:
            if (label != 0):
                print("Found esb_anomaly in success rate")
                return True
        
        self.update_esb_data(esb_tmp)

        return False

    def trace_processing(self):
        print("Started trace processing")
        dftmp = self.trace_data[self.trace_data['callType']=='RemoteProcess']
        dftmp = dftmp[['pid','cmdb_id']]
        dftmp = dftmp.set_index('pid')
        csf_dict = dftmp.to_dict()
        csf_cmdb = {str(key):str(values) for key, values in csf_dict['cmdb_id'].items()}
        for index, row in self.trace_data.iterrows():
            if row['id'] in csf_cmdb:
                self.trace_data.at[index, 'cmdb_id'] = csf_cmdb[row['id']]

        elapsed_time = {}
        children = {}
        parent_service = {}
        for index, row in self.trace_data.iterrows():
            if row['pid'] != 'None':
                if row['pid'] in children.keys():
                    children[row['pid']].append(row['id'])
                else:
                    children[row['pid']] = [row['id']]
            elapsed_time [row['id']] = float(row['elapsedTime'])
            parent_service[row['id']] = row['serviceName']

        # self.trace_data['actual_time'] = 0.0
        self.trace_data['path'] = ''
        for index, row in self.trace_data.iterrows():
            total_child = 0.0
            if row['pid'] not in parent_service.keys():
                self.trace_data.at[index, 'path'] = 'Start-' + row['serviceName']
            else:
                self.trace_data.at[index, 'path'] = parent_service[row['pid']] + '-' + row['serviceName']

            # if row['id'] not in children.keys():
            #     self.trace_data.at[index, 'actual_time'] = row['elapsedTime']
            #     continue
            # for child in children[row['id']]:
            #     total_child += elapsed_time[child]
            # self.trace_data.at[index, 'actual_time'] = row['elapsedTime'] - total_child
        
        print("Trace processed")


# Three topics are available: platform-index, business-index, trace.
# Subscribe at least one of them.
AVAILABLE_TOPICS = set(['platform-index', 'business-index', 'trace'])
CONSUMER = KafkaConsumer('platform-index', 'business-index', 'trace',
                         bootstrap_servers=['172.21.0.8', ],
                         auto_offset_reset='latest',
                         enable_auto_commit=False,
                         security_protocol='PLAINTEXT')


class Trace():  # pylint: disable=invalid-name,too-many-instance-attributes,too-few-public-methods
    '''Structure for traces'''

    __slots__ = ['call_type', 'start_time', 'elapsed_time', 'success',
                 'trace_id', 'id', 'pid', 'cmdb_id', 'service_name', 'ds_name']

    def __new__(self, data):
        self.trace = data
        if self.trace['callType'] == 'JDBC':
            try :
                self.trace['serviceName'] = data['dsName']
            except:
                print(data)
                print('JDBC doesnt have dsName')

        if 'dsName' in self.trace:
            self.trace.pop('dsName')

        return self.trace


def detection(timestamp):
    print('Starting Anomaly Detection')
    startTime = timestamp - 180000  # one minute before anomaly

    esb_df_temp = esb_df[(esb_df['startTime'] >= startTime) &
                         (esb_df['startTime'] <= timestamp)]
    trace_df_temp = trace_df[(trace_df['startTime'] >= startTime) &
                             (trace_df['startTime'] <= timestamp)]
    host_df_temp = host_df[(host_df['timestamp'] >= startTime) &
                           (host_df['timestamp'] <= timestamp)]

    rca_temp = MicroRCA(esb_df_temp, trace_df_temp, host_df_temp)
    results_to_send_off = rca_temp.run()

    print('Anomaly Detection Done.')
    if len(results_to_send_off) == 0:
        print('Nothing detected')
        return False
    # for a in anom_hosts:
    #     item = a.split(':')[0]
    #     if (item not in anoms):
    #         anoms.append(item)
    print(results_to_send_off)
    submit(results_to_send_off)
    return True


def submit(ctx):
    '''Submit answer into stdout'''
    # print(json.dumps(data))
    assert (isinstance(ctx, list))
    for tp in ctx:
        assert(isinstance(tp, list))
        assert(len(tp) == 2)
        assert(isinstance(tp[0], str))
        assert(isinstance(tp[1], str) or (tp[1] is None))
    data = {'content': json.dumps(ctx)}
    r = requests.post(
        'http://172.21.0.8:8000/standings/submit/', data=json.dumps(data))


def main():
    '''Consume data and react'''
    assert AVAILABLE_TOPICS <= CONSUMER.topics(), 'Please contact admin'

    global esb_df, host_df, trace_df

    print('Started receiving data! Fingers crossed...')

    # Dataframes for the three different datasets
    esb_df = pd.DataFrame(columns=[
                          'serviceName', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate'])
    host_df = pd.DataFrame(
        columns=['itemid', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
    trace_df = pd.DataFrame(columns=['callType', 'startTime', 'elapsedTime',
                                     'success', 'traceId', 'id', 'pid', 'cmdb_id', 'serviceName'])

    rca = MicroRCA(esb_df)
    esb_anomaly = False
    
    timenow = 0
    a_time = time.time()
    for message in CONSUMER:
        data = json.loads(message.value.decode('utf8'))

        # Host data
        if message.topic == 'platform-index':
            timenow = data['timestamp']
            for stack in data['body']:
                for item in data['body'][stack]:
                    host_df = host_df.append(item, ignore_index=True)

        # ESB data
        elif message.topic == 'business-index':
            timenow = data['startTime']

            for item in data['body']['esb']:
                esb_df = esb_df.append(item, ignore_index=True)
                esb_anomaly = rca.analyze_esb(item)

            if time.time() - a_time >= 600 and esb_anomaly:
                print("oops")
                result = detection(timestamp)
                if result:
                    a_time =  time.time()

        # Trace data
        else:  # message.topic == 'trace'
            timestamp = data['startTime']
            trace_df = trace_df.append(Trace(data), ignore_index=True)
        
        esb_df = esb_df[esb_df.startTime >= timenow-600000]
        host_df = host_df[host_df.timestamp >= timenow-600000]
        trace_df = trace_df[trace_df.startTime >= timenow-600000]


if __name__ == '__main__':
    main()
