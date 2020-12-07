#!/usr/bin/env python3


'''
Example for data consuming.
'''
import requests
import json

from kafka import KafkaConsumer

import pandas as pd
import numpy as np
import time
import networkx as nx
from sklearn.cluster import Birch
from sklearn import preprocessing
import numpy as np


# Three topics are available: platform-index, business-index, trace.
# Subscribe at least one of them.
AVAILABLE_TOPICS = set(['platform-index', 'business-index', 'trace'])
CONSUMER = KafkaConsumer('platform-index', 'business-index', 'trace',
                         bootstrap_servers=['172.21.0.8', ],
                         auto_offset_reset='latest',
                         enable_auto_commit=False,
                         security_protocol='PLAINTEXT')

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
                ## What if no root cause. ['host', None] is possible too

        print("Root-cause detection finished with %d causes\n" %
              (len(root_causes)))
        return root_causes

class PlatformIndex():  # pylint: disable=too-few-public-methods
    '''Structure for platform indices'''

    __slots__ = ['item_id', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id']

    def __init__(self, data):
        self.item_id = data['itemid']
        self.name = data['name']
        self.bomc_id = data['bomc_id']
        self.timestamp = data['timestamp']
        self.value = data['value']
        self.cmdb_id = data['cmdb_id']

    def __new__(self, data):
        # self.host = dict()
        # self.host['itemid'] = data['itemid']
        # self.host['name'] = data['name']
        # self.host['bomc_id'] = data['bomc_id']
        # self.host['timestamp'] = data['timestamp']
        # self.host['value'] = data['value']
        # self.host['cmdb_id'] = data['cmdb_id']
        return data


class BusinessIndex():  # pylint: disable=too-few-public-methods
    '''Structure for business indices'''

    __slots__ = ['service_name', 'start_time', 'avg_time', 'num',
                 'succee_num', 'succee_rate']

    def __init__(self, data):
        self.service_name = data['serviceName']
        self.start_time = data['startTime']
        self.avg_time = data['avg_time']
        self.num = data['num']
        self.succee_num = data['succee_num']
        self.succee_rate = data['succee_rate']
    
    def __new__(self, data):
        # self.esb = dict()
        # self.esb['serviceName'] = data['serviceName']
        # self.esb['startTime'] = data['startTime']
        # self.esb['avg_time'] = data['avg_time']
        # self.esb['num'] = data['num']
        # self.esb['succee_num'] = data['succee_num']
        # self.esb['succee_rate'] = data['succee_rate']
        return data


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

        if self.call_type == 'JDBC':
        # if 'dsName' in data:
            # For data['callType'] in ['JDBC', 'LOCAL']
            # self.ds_name = data['dsName']
            self.service_name = data['dsName']

    def __new__(self, data):
        self.trace = data
        # self.trace = dict()
        # self.trace['callType'] = data['callType']
        # self.trace['startTime'] = data['startTime']
        # self.trace['elapsedTime'] = data['elapsedTime']
        # self.trace['success'] = data['success']
        # self.trace['traceId'] = data['traceId']
        # self.trace['id'] = data['id']
        # self.trace['pid'] = data['pid']
        # self.trace['cmdb_id'] = data['cmdb_id']

        # if 'serviceName' in data:
        #     # For data['callType']
        #     #  in ['CSF', 'OSB', 'RemoteProcess', 'FlyRemote', 'LOCAL']
        #     self.trace['serviceName'] = data['serviceName']

        if self.trace['callType'] == 'LOCAL':
        # if 'dsName' in data:
            # For data['callType'] in ['JDBC', 'LOCAL']
            # self.ds_name = data['dsName']
            self.trace['serviceName'] = data['dsName']
            del self.trace['dsName']
        return self.trace

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
    r = requests.post('http://172.21.0.8:8000/standings/submit/', data=json.dumps(data))


def main():
    '''Consume data and react'''
    # Check authorities
    assert AVAILABLE_TOPICS <= CONSUMER.topics(), 'Please contact admin'

    print('Start Running')

    # submit([['docker_003', 'container_cpu_used']])
    esb_df = pd.DataFrame(columns = ['serviceName', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate'])
    host_df = pd.DataFrame(columns = ['itemid', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
    trace_df = pd.DataFrame(columns = ['callType', 'startTime', 'elapsedTime', 'success', 'traceId', 'id', 'pid', 'cmdb_id', 'serviceName'])
    

    # i = 0
    for message in CONSUMER:
        # i += 1
        data = json.loads(message.value.decode('utf8'))
        if message.topic == 'platform-index':
            # # data['body'].keys() is supposed to be
            # # ['os_linux', 'db_oracle_11g', 'mw_redis', 'mw_activemq',
            # #  'dcos_container', 'dcos_docker']
            # data = {
            #     'timestamp': data['timestamp'],
            #     'body': {
            #         stack: [PlatformIndex(item) for item in data['body'][stack]]
            #         for stack in data['body']
            #     },
            # }
            # timestamp = data['timestamp']

            for stack in data['body']:
                if stack == 'os_linux' or stack == 'dcos_docker':
                    for item in data['body'][stack]:
                        host_df = host_df.append(item, ignore_index=True)
                        # print('host: ', item)

        elif message.topic == 'business-index':
            # # data['body'].keys() is supposed to be ['esb', ]
            # data = {
            #     'startTime': data['startTime'],
            #     'body': {
            #         key: [BusinessIndex(item) for item in data['body'][key]]
            #         for key in data['body']
            #     },
            # }
            # timestamp = data['startTime']
            anomaly = False
            timestamp = data['startTime']
            for item in data['body']['esb']:
                esb_df = esb_df.append(item, ignore_index=True)
                # print('esb: ', item)
                if item['succee_rate'] < 1 or item['avg_time'] > 0.67:
                    anomaly = True

            if anomaly:
                result = detection(timestamp, trace_df, host_df)
                if result:
                    esb_df = pd.DataFrame(columns = ['serviceName', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate'])
                    host_df = pd.DataFrame(columns = ['itemid', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
                    trace_df = pd.DataFrame(columns = ['callType', 'startTime', 'elapsedTime', 'success', 'traceId', 'id', 'pid', 'cmdb_id', 'serviceName'])

        else:  # message.topic == 'trace'
            # data = {
            #     'startTime': data['startTime'],
            #     'body': Trace(data),
            # }
            # timestamp = data['startTime']
            trace_df = trace_df.append(Trace(data), ignore_index=True)
            # if 'dsName' in data:
            #     print('trace: ', data)
        # print(i, message.topic, timestamp)

def detection(timestamp, trace_df, host_df):
    print('Starting Anomay Detection')
    startTime = timestamp - 600000
    trace_df = trace_df[(trace_df['startTime']>=startTime)&(trace_df['startTime']<=timestamp)]
    host_df = host_df[(host_df['timestamp']>=startTime)&(host_df['timestamp']<=timestamp)]

    RCA = MicroRCA(trace_df, smoothing_window=3, threshold=0.03)
    RCA.data_processing()
    anom_hosts = RCA.anomaly_detection()

    if len(anom_hosts) == 0:
        return False

    anoms = []
    for a in anom_hosts:
        item = a.split(':')[0]
        if (item not in anoms):
            anoms.append(item)
    
    root_causes = RCA.find_root_causes(host_df, anoms)
    print('Anomaly Detection Done.')

    if len(root_causes)==1:
        output = [root_causes[0]]
    elif len(root_causes)>1:
        output = [root_causes[0],root_causes[1]]
    elif len(root_causes)==0:
        output = [[anoms[0], None]]
    submit(output)
    return True

if __name__ == '__main__':
    main()