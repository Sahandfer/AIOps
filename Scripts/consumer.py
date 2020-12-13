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


class RCA():

    def __init__(self, esb_data, trace_data, host_data, use_actual_time=True, use_trace_avg=False, use_KDE=False):

        self.esb_data = esb_data
        self.trace_data = trace_data
        self.host_data = host_data
        self.time_column = 'actual_time' if use_actual_time else 'elapsedTime'
        self.use_trace_avg = use_trace_avg
        self.use_KDE = use_KDE

        self.base_graph = nx.DiGraph()
        self.times_dict = {}
        self.trace_processing()
        self.edges = list(self.trace_data.path.unique())

        if use_trace_avg:
            for edge in self.edges:
                data_subset = self.trace_data.loc[self.trace_data.path == edge].copy()
                data_subset['time_group'] = data_subset.startTime//60000
                averaged_data = data_subset.groupby(
                    'time_group')[self.time_column].mean()
                self.times_dict[edge] = list(averaged_data)
        else:
            for edge in self.edges:
                data_subset = self.trace_data.loc[self.trace_data.path == edge]
                self.times_dict[edge] = list(
                    data_subset[self.time_column])

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

    def analyze_esb(self):
        
        values = self.esb_data['avg_time'].tolist()
        print(values)
        birch_labels_time = self.birch(values)
        # birch_labels_rate = self.birch(self.esb_data['avg_time'])
        for label in birch_labels_time:
            if (label != 0):
                print("Found esb_anomaly in avg_time")
                return True

        values = self.esb_data['succee_rate'].tolist()
        print(values)
        birch_labels_time = self.birch(values)
        for label in birch_labels_time:
            if (label != 0):
                print("Found esb_anomaly in success rate")
                return True
                
        return False

    def trace_processing(self):
        print("Started trace processing")

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

        self.trace_data['actual_time'] = 0.0
        self.trace_data['path'] = ''
        for index, row in self.trace_data.iterrows():
            total_child = 0.0
            if row['pid'] not in parent_service.keys():
                self.trace_data.at[index, 'path'] = 'Start-' + row['serviceName']
            else:
                self.trace_data.at[index, 'path'] = parent_service[row['pid']] + '-' + row['serviceName']

            if row['id'] not in children.keys():
                self.trace_data.at[index, 'actual_time'] = row['elapsedTime']
                continue
            for child in children[row['id']]:
                total_child += elapsed_time[child]
            self.trace_data.at[index, 'actual_time'] = row['elapsedTime'] - total_child
        
        print("Trace processed")
        

    def run(self):
        # run root cause analysis on the data given
        self.create_graph()
        # self.personalize_graph()
        dodgy_node = self.page_rank()
        # print('Pagerank suggests the problematic service is: %s.' % dodgy_node)
        root_causes = self.analyse_host_data(dodgy_node)
        result_to_send_off = []
        for host, kpi, _ in root_causes[:2]:
            result_to_send_off.append([host, kpi])
        # print('Thus the result to be sent off to the server is:')
        # print(result_to_send_off)
        # for node in dodgy_nodes:
        #     root_causes = self.analyse_host_data(node)
        #     result_to_send_off = []
        #     for host, kpi, _ in root_causes[:2]:
        #         result_to_send_off.append([host, kpi])
        #     print('Thus the result to be sent off to the server is:')
        #     print(result_to_send_off)
        return result_to_send_off

    def create_graph(self):
        # creates weighted graph from the trace data
        print("Creating graph")

        for edge in self.edges:
            source, destination = edge.split('-')
            if source != 'Start':
                vector_of_time = self.times_dict[edge]
                reshaped_vector_of_time = np.reshape(vector_of_time, (-1, 1))
                if len(reshaped_vector_of_time) > 5000:
                    k = len(reshaped_vector_of_time) // 5000 + 1
                    rnge = np.arange(len(reshaped_vector_of_time))
                    indices = (rnge % k) == 0
                    reshaped_vector_of_time = reshaped_vector_of_time[indices]
                KDE = KernelDensity(kernel='gaussian', bandwidth=1.0).fit(
                    reshaped_vector_of_time)
                KDE_scores = KDE.score_samples(reshaped_vector_of_time)
                mean_of_KDE_scores = - np.mean(KDE_scores)

                normalized_vector_of_time = preprocessing.normalize(
                    [vector_of_time]).reshape(-1, 1)
                birch = Birch(n_clusters=None, threshold=0.1,
                              compute_labels=True)
                birch.fit(normalized_vector_of_time)
                birch.predict(normalized_vector_of_time)
                labels = birch.labels_
                birch_clustering_score = 100 * \
                    len(labels[np.where(labels != 0)])/len(labels)

                total_weight = mean_of_KDE_scores * birch_clustering_score + \
                    mean_of_KDE_scores + birch_clustering_score

                self.base_graph.add_edge(
                    source, destination, weight=total_weight)
                # print('Added edge: %s with weight %f, ' % (edge, total_weight) + 'KDE performed on %d rows' % len(reshaped_vector_of_time))
        print('Finished creating graph.')

    def personalize_graph(self):
        self.personalization = {}
        for node in self.base_graph.nodes:
            self.personalization[node] = self.base_graph.in_degree(
                node)/self.base_graph.out_degree(node)
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
        # use pagerank to locate the problematic service
        # page_rank = nx.pagerank(self.base_graph, alpha=0.85, personalization = self.personalization ,max_iter=10000)
        # page_rank = [(svc, val) for svc, val in dict(sorted(page_rank.items(), key=lambda item: item[1], reverse=True)).items()]
        print("Starting pagerank")
        page_rank = []
        for node in self.base_graph.nodes:
            weight = 0
            for _, _, d in self.base_graph.in_edges(node, data=True):
                weight += d['weight']
            val = weight
            page_rank.append((node, val))

        page_rank.sort(key=lambda tripple: tripple[1], reverse=True)
        # print('All nodes listed by their rank:')
        # for svc, val in page_rank:
        # print('Service name: ' + svc + ', score: %f' % val)
        print("Pagerank finished")
        if (len(page_rank)):
            if (len(page_rank[0])):
                return page_rank[0][0]
        return False

    def analyse_host_data(self, dodgy_node):
        print("Analyzing host data")

        # given a problematic service, look for the root cause in the service's host data.
        dodgy_hosts = self.trace_data.loc[self.trace_data.serviceName ==
                                          dodgy_node].cmdb_id.unique()
        host_groups = self.host_data[self.host_data['cmdb_id'].isin(
            dodgy_hosts)].groupby('cmdb_id')[['name', 'value']]

        # host_groups = self.host_data[self.host_data['cmdb_id'].isin([dodgy_node])].groupby('cmdb_id')[['name', 'value']]

        root_causes = []
        for host, _ in host_groups:
            root_causes_for_host = []
            df = host_groups.get_group(host)
            name_groups = df.groupby('name')['value'].apply(
                list).reset_index(name='values')
            for i in range(len(name_groups)):
                row = name_groups.iloc[i]
                name = row['name']
                values = row['values']
                if len(set(values)) > 1:
                    outliers, score = self.find_outliers(values)
                    if outliers:
                        root_causes_for_host.append((name, score))

            if len(root_causes_for_host) > 0:
                KPI_name, score = max(
                    root_causes_for_host, key=lambda item: item[1])
                root_causes.append((host, KPI_name, score))

        root_causes.sort(key=lambda tripple: tripple[2], reverse=True)
        # print('Possible route causes:')
        # for host, KPI_name, KDE_score in root_causes:
        # print('Host: ' + host + ', KPI name: ' + KPI_name + ', Score: %f' % KDE_score)
        print("Data analysis finished")

        return root_causes

    def find_outliers(self, values):
        # flag if a KPI is exhibiting anomalous behaviour
        if self.use_KDE:
            self.kde(values)
        else:
            normalized_values = preprocessing.normalize(
                [values]).reshape(-1, 1)
            birch = Birch(n_clusters=None, threshold=0.06, compute_labels=True)
            birch.fit(normalized_values)
            birch.predict(normalized_values)
            labels = birch.labels_
            birch_clustering_score = len(
                labels[np.where(labels != 0)])/len(labels)
            return (birch_clustering_score > 0), birch_clustering_score


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

    def __init__(self, data):
        self.call_type = data['callType']
        self.start_time = data['startTime']
        self.elapsed_time = data['elapsedTime']
        self.success = data['success']
        self.trace_id = data['traceId']
        self.id = data['id']
        self.pid = data['pid']
        self.cmdb_id = data['cmdb_id']

        if 'serviceName' in data:
            # For data['callType']
            #  in ['CSF', 'OSB', 'RemoteProcess', 'FlyRemote', 'LOCAL']
            self.service_name = data['serviceName']

        if self.call_type in ['JDBC', 'LOCAL']:
            self.service_name = data['dsName']

    def __new__(self, data):
        return data


def detection(timestamp):
    print('Starting Anomaly Detection')
    startTime = timestamp - 60000  # one minute before anomaly

    esb_df_temp = esb_df[(esb_df['startTime'] >= startTime) &
                         (esb_df['startTime'] <= timestamp)]
    trace_df_temp = trace_df[(trace_df['startTime'] >= startTime) &
                             (trace_df['startTime'] <= timestamp)]
    host_df_temp = host_df[(host_df['timestamp'] >= startTime) &
                           (host_df['timestamp'] <= timestamp)]

    rca_temp = RCA(esb_df_temp, trace_df_temp, host_df_temp, True, True, False)
    results_to_send_off = rca_temp.run()

    print(results_to_send_off)
    print('Anomaly Detection Done.')
    if len(results_to_send_off) == 0:
        return False

    # for a in anom_hosts:
    #     item = a.split(':')[0]
    #     if (item not in anoms):
    #         anoms.append(item)
    print('Nothing detected')
    # submit(root_causes)
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

    rca = RCA(esb_df, trace_df, host_df, True, True, False)
    esb_anomaly = False

    for message in CONSUMER:
        data = json.loads(message.value.decode('utf8'))

        # Host data
        if message.topic == 'platform-index':
            for stack in data['body']:
                if stack == 'os_linux' or stack == 'dcos_docker':
                    for item in data['body'][stack]:
                        host_df = host_df.append(item, ignore_index=True)

        # ESB data
        elif message.topic == 'business-index':
            timestamp = data['startTime']

            for item in data['body']['esb']:
                esb_df = esb_df.append(item, ignore_index=True)
                rca.update_esb_data(esb_df)
                esb_anomaly = rca.analyze_esb()

            if esb_anomaly:
                print("oops")
                result = detection(timestamp)
                if result:
                    esb_anomaly = False
                    esb_df = pd.DataFrame(columns=[
                                          'serviceName', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate'])
                    host_df = pd.DataFrame(
                        columns=['itemid', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
                    trace_df = pd.DataFrame(columns=[
                                            'callType', 'startTime', 'elapsedTime', 'success', 'traceId', 'id', 'pid', 'cmdb_id', 'serviceName'])

        # Trace data
        else:  # message.topic == 'trace'
            trace_df = trace_df.append(Trace(data), ignore_index=True)


if __name__ == '__main__':
    main()
