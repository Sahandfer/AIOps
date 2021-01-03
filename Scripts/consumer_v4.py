#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Example for RCAing like a B0$$
'''
import requests
import json
from kafka import KafkaConsumer
import itertools
import pickle
import time
import numpy as np
import pandas as pd
from scipy.stats import t
from termcolor import colored
from threading import Thread
from collections import defaultdict


class RCA():
    '''
    Root Cause Analysis class for detecting the root cause
    '''
    def __init__(self, trace_data, host_data, alpha=0.99, ub=0.1, take_minute_averages_of_trace_data=True, division_milliseconds=60000):
        self.trace_data = trace_data
        self.host_data = host_data
        self.alpha = alpha
        self.ub = ub
        self.anomaly_chart = None
        self.take_minute_averages_of_trace_data = take_minute_averages_of_trace_data
        self.division_milliseconds = division_milliseconds

    def run(self):
        '''
        Runs RCA and gets the output to send to the server.

        Output: list of anomalies/None to be sent off to server
        '''
        overall_start_time = time.time()
        print('Running RCA on %d trace data rows and %d host data rows' %
              (len(self.trace_data), len(self.host_data)))

        print('Computing for anomaly score chart...')
        self.hesd_trace_detection(alpha=self.alpha, ub=self.ub)
        print('Score chart computed!')

        self.local_initiate()
        output = self.find_anomalous_hosts()

        if not output is None:
            print(self.anomaly_chart.to_dict())

        print('The output to send to the server is: ' +
              colored(str(output), 'magenta'))

        print('RCA finished in ' + colored('%f', 'cyan') %
              (time.time() - overall_start_time) + ' seconds.')
        return output

    def esd_test_statistics(self, x, hybrid=True):
        '''
        Compute the location and dispersion sample statistics used to carry out the ESD test.

        x : List, array or series containing the time series data

        Output: the two statistics used to carry out ESD
        '''
        if hybrid:
            location = np.ma.median(x) # Median
            dispersion = np.ma.median(np.abs(x - np.median(x))) # Median Absolute Deviation
        else:  
            location = np.ma.mean(x) # Mean
            dispersion = np.ma.std(x) # Standard Deviation
            
        return location, dispersion

    def esd_test(self, x, alpha=0.95, ub=0.499, hybrid=True):
        '''
        Carries out the Extreme Studentized Deviate(ESD) test which can be used to detect one or more outliers present in the timeseries

        x      : List, array, or series containing the time series
        freq   : Int that gives the number of periods per cycle (7 for week, 12 for monthly, etc)
        alpha  : Confidence level in detecting outliers
        ub     : Upper bound on the fraction of datapoints which can be labeled as outliers (<=0.499)
        
        Output : The score of a call over the past 20 mins of data
        '''       
        x = [p for p in x if p==p]
        nobs = len(x)
        if ub > 0.4999:
            ub = 0.499
        # Maximum number of anomalies. At least 1 anomaly must be tested.
        k = max(int(np.floor(ub * nobs)), 1)
        # The "ma" structure allows masking of values to exclude the elements from any calculation
        res = np.ma.array(x, mask=False)
        anomalies = []
        med = np.median(x)
        for i in range(1, k+1):
            location, dispersion = self.esd_test_statistics(res, hybrid)
            tmp = np.abs(res - location) / dispersion
            idx = np.argmax(tmp)  # Index of the test statistic
            test_statistic = tmp[idx]
            n = nobs - res.mask.sum()  # sums  nonmasked values
            critical_value = (n - i) * t.ppf(alpha, n - i - 1) / np.sqrt((n - i - 1 + np.power(t.ppf(alpha, n - i - 1), 2)) * (n - i - 1))
            if test_statistic > critical_value:
                anomalies.append((x[idx]-med) / med)
            res.mask[idx] = True
        if len(anomalies) == 0:
            return 0
        return np.nanmean(np.abs(anomalies))

    def hesd_trace_detection(self, alpha=0.95, ub=0.02):
        '''
        Hybrid ESD Detection
        Use Hybrid ESD on the trace data to create the anomaly chart.
        Takes averages over 1 minute intervals for the ESD score.

        alpha : alpha value for the ESD
        ub    : ub value for ESD

        Output: The anomaly chart table (pd.DataFrame())
        '''
        grouped_df = self.trace_data.groupby(['cmdb_id', 'serviceName'])[['startTime', 'success', 'elapsedTime']]

        self.anomaly_chart = pd.DataFrame()
        for (a, b), value in grouped_df:
            failure = 0
            if 'db' in b:
                failure = sum(value['success']==False)*5
            value['time_group'] = value.startTime//self.division_milliseconds
            value = value.groupby(['time_group'])['elapsedTime'].mean().reset_index()
            result = self.esd_test(value['elapsedTime'].to_numpy(), alpha=alpha, ub=ub, hybrid=False)
            self.anomaly_chart.loc[b, a] = result + failure

        self.anomaly_chart = self.anomaly_chart.sort_index()
        # print(self.anomaly_chart)

        # import networkx as nx
        # import matplotlib.pyplot as plt
        # dg = nx.DiGraph()
        # for col in self.anomaly_chart:
        #     for index, row in self.anomaly_chart.iterrows():
        #         if str(row[col]) != 'nan':
        #             dg.add_edge(col, index)

        # positions = {}
        # positions['os_022'] = (1,4)
        # positions['os_021'] = (4,4)
        # positions['docker_002'] = (0.5,3)
        # positions['docker_001'] = (1.5,3)
        # positions['docker_003'] = (3.5,3)
        # positions['docker_004'] = (4.5,3)
        # positions['docker_005'] = (5,2)
        # positions['docker_006'] = (4,2)
        # positions['docker_007'] = (0,2)
        # positions['docker_008'] = (1,2)
        # positions['db_007'] = (2,2)
        # positions['db_009'] = (3,2)
        # positions['db_003'] = (2.5,1)
        # nx.draw_networkx(dg, positions, node_size = 5500, node_color = '#00BFFF')
        # plt.show()

        # print(self.anomaly_chart.to_dict())
        return self.anomaly_chart

    def local_initiate(self):
        '''
        Sets up various lists used later.
        '''
        self.dockers = ['docker_001', 'docker_002', 'docker_003', 'docker_004',
                        'docker_005', 'docker_006', 'docker_007', 'docker_008']
        self.docker_hosts = ['os_017', 'os_018', 'os_019', 'os_020']

        self.docker_kpi_names = ['container_cpu_used', None]
        self.os_kpi_names = ['Sent_queue', 'Received_queue']
        self.db_kpi_names = ['Proc_User_Used_Pct', 'Proc_Used_Pct', 'Sess_Connect', 'On_Off_State', 'tnsping_result_time']

        self.docker_lookup_table = {}
        for i in range(len(self.dockers)):
            self.docker_lookup_table[self.dockers[i]] = self.docker_hosts[i % 4]

    def find_anomalous_hosts(self, min_threshold=10):
        '''
        Find Any Anomalous Hosts
        Searches the anomaly chart for the hosts most likely to be causing the anomaly.
        min_threshold: Minimum threshold value for the search. The threshold is the
                       value over which entries are considered anomalous

        Output       : dictionary of anomalous hosts and a dictionary that states if each
                       host has local anomalous behaviour (used for docker hosts)
        '''
        table = self.anomaly_chart.copy()
        # get a threshold. Either a qarter of the max value in the entire table or the min_threshold value
        threshold = max(0.2 * table.stack().max(), min_threshold)
        print('The largest value in the anomaly chart is: %f' % table.stack().max())
        print('The threshold is: %f' % threshold)

        local_abnormal = {}
        # these dictionaries store the number of anomalies in the rows/columns of a host respectively
        row_dict = {}
        column_dict = {}
        # these dictionaries store the values of the rows/columns of a host respectively
        row_confidence_dict = {}
        column_confidence_dict = {}

        check_for_most_simple_case = sum(np.array(table.stack()) >= threshold)
        if check_for_most_simple_case == 1:
            for column in table:
                for index, row in table.iterrows():
                    if row[column] >= threshold:
                        print('Only 1 anomalous value in the table, so we localise on the row name: %s' % index)
                        return self.find_anomalous_kpi(index, index == column)

        for column in table:
            for index, row in table.iterrows():
                increment = 0
                if row[column] >= threshold:
                    # add a 'count' for each row/column entry that is anomalous using increment
                    increment = 1
                    # initialise dictionary to avoid erros in the future
                    local_abnormal[column] = local_abnormal.get(column, False)
                    local_abnormal[index] = local_abnormal.get(index, False)
                    if column == index:
                        # if one of the diagonals of the table is anomalous, we mark it here
                        local_abnormal[column] = True

                column_dict[column] = column_dict.get(column, 0)
                column_dict[column] += increment
                column_confidence_dict[column] = column_confidence_dict.get(column, [])
                column_confidence_dict[column].append(row[column])

                row_dict[index] = row_dict.get(index, 0)
                row_dict[index] += increment
                row_confidence_dict[index] = row_confidence_dict.get(index, [])
                row_confidence_dict[index].append(row[column])

        for key, value in column_confidence_dict.items():
            # take the sum of the entries in a column
            column_confidence_dict[key] = np.nansum(value)

        for key, value in row_confidence_dict.items():
            # take the sum of the entries in a row
            row_confidence_dict[key] = np.nansum(value)

        final_dict = {}
        for key in list(row_dict.keys()):
            if key in list(column_dict.keys()):
                # row_dict now contains the mean count of the number of anomalies in the rows and columns of a host
                # use integer division to dismiss hosts with only one anomalous non-diagonal value in the table
                row_dict[key] = (row_dict[key] + column_dict[key]) // 2
                row_confidence_dict[key] = (row_confidence_dict[key] + column_confidence_dict[key]) / 2
            # multiply the mean number of anomlies by the score of the rows/columns of a host
            # if the number of anomalies (row_dict[key]) is 0, we get 0.
            final_dict[key] = row_dict[key] * row_confidence_dict[key]

        for key in set(column_confidence_dict.keys()).difference(set(row_confidence_dict.keys())):
            # if there is missing host data, we might miss out anomalies on weird tables. Thus we
            # check the column keys. This code usually does not run, as the set difference is empty.
            final_dict[key] = column_dict[key] * column_confidence_dict[key]
        
        # sort the potential anomalies by their score
        dodgy_hosts = dict(sorted(final_dict.items(), key=lambda item: item[1], reverse=True))
        # filter out unlikely anomalies by taking 10% of the max of the anomaly scores, or 1
        m = 0.1 * max(list(dodgy_hosts.values())+[10])
        dodgy_hosts = {k: v for k, v in dodgy_hosts.items() if (v > m)}

        output = self.localize(dodgy_hosts, local_abnormal)
        return output

    def find_anomalous_kpi(self, cmdb_id,  local_abnormal):
        '''
        Find the KPIs Responsible for the Anomaly
        Given an anomalous host, return the root cause KPIs for that host
        in the output format specified by the project.
        cmdb_id        : the cmdb_id of the host
        local_abnormal : boolean indicating if (for docker hosts) the anomaly is
                         on local calls or not

        output         : list to send off to server
        '''
        # two inputs, cmdb_id and local_abnormal. cmdb_id is the host we sent to server, local_abnormal is a boolean used for 'docker' anomalies.
        kpi_names = []
        if 'os' in cmdb_id:
            # os is always just ['Sent_queue', 'Received_queue']
            kpi_names = self.os_kpi_names
        elif 'docker' in cmdb_id:
            # if the local method is abnormal, i.e. the self-calling function is abnormal, it is a cpu fault
            if local_abnormal:
                kpi_names = ['container_cpu_used']
            else:
                # if the self calling function is not abnormal, it is a network error
                kpi_names = [None]
        else:
            kpi_names = self.db_kpi_names
            host_data_subset = self.host_data.loc[(self.host_data.cmdb_id == cmdb_id) & (self.host_data.name == 'On_Off_State')]
            check = any(host_data_subset.value < 1)
            if check:
                kpi_names = kpi_names[3:]
            else:
                kpi_names = kpi_names[:3]

        to_be_sent = []
        for kpi in kpi_names:
            to_be_sent.append([cmdb_id, kpi])

        return to_be_sent

    def localize(self, dodgy_host_dict, local_abnormal):
        '''
        Localize the Anomalous Hosts
        Narrow down the anomalous hosts given by 'find_anomalous_hosts' and look for 
        similar hosts or dependancies
        dodgy_host_dict : dictionary in the format {host: score}
        local_abnormal  : dictionary in the format {host: boolean} where the boolean
                          indicates if (for docker hosts) the anomaly occurs on local
                          calls
        
        output          : result to send off to server
        '''
        dodgy_hosts = list(dodgy_host_dict.keys())
        n = len(dodgy_host_dict)
        print('We found %d anomalies, printed below:' % n)
        print(dodgy_host_dict)
        if n < 1:
            return None
        else:
            # create lists containing all the potential os and docker anomalies
            os = [x for x in dodgy_hosts if 'os' in x]
            docker = [y for y in dodgy_hosts if 'docker' in y]

            if len(os) == 2:
                # two os means it must be os_001
                to_be_sent = self.find_anomalous_kpi('os_001', False)
                return to_be_sent

            if len(docker) >= 2:
                # 2 or more potential docker anomalies, so we check if its a db_003 first
                if len(docker) >= 4:
                    if sorted(docker)[-4:] == ['docker_005', 'docker_006', 'docker_007', 'docker_008']:
                        to_be_sent = self.find_anomalous_kpi('db_003', False)
                        return to_be_sent
                # if we reach here, its not a db_003, so it might be an os_00x
                c = list(itertools.combinations(docker, 2))
                print('The combinations of docker hosts are printed below:')
                print(c)
                for a, b in c:
                    shared_host = self.docker_lookup_table[a]
                    if shared_host == self.docker_lookup_table[b]:
                        to_be_sent = self.find_anomalous_kpi(
                            shared_host, False)
                        return to_be_sent

            if 'fly' in dodgy_hosts[0]:
                # fly remote means it must be os_009
                to_be_sent = self.find_anomalous_kpi('os_009', False)
                return to_be_sent
            else:
                # if there are 2 or more potential anomalies and there are no similarities, we simply
                # return the 'most anomalous' one
                to_be_sent = self.find_anomalous_kpi(
                    dodgy_hosts[0], local_abnormal[dodgy_hosts[0]])
                return to_be_sent


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

        if self.trace['callType'] == 'JDBC' or self.trace['callType'] == 'LOCAL':
            try:
                self.trace['serviceName'] = data['dsName']
            except:
                print(data)
                print('JDBC doesnt have dsName')

        elif self.trace['callType'] == 'RemoteProcess' or self.trace['callType'] == 'OSB':
            self.trace['serviceName'] = data['cmdb_id']

        if 'dsName' in self.trace:
            self.trace.pop('dsName')

        return self.trace


def detection(timestamp):
    '''
    Anomaly Detection
    Takes the last 20 mins of data and runs RCA. Then sends the result off to the server.
    '''
    global host_df, trace_df
    print('Starting Anomaly Detection')
    # startTime = timestamp - 1200000  # 20 minutes before anomaly

    # trace_df_temp = trace_df[(trace_df['startTime'] >= startTime) &
    #                          (trace_df['startTime'] <= timestamp)]
    # host_df_temp = host_df[(host_df['timestamp'] >= startTime) &
    #                        (host_df['timestamp'] <= timestamp)]

    if (len(host_df) == 0) or (len(trace_df) == 0):
        print('Error: empty dataframe')

    rca_temp = RCA(trace_data=trace_df, host_data=host_df, alpha=0.99, ub=0.1)
    results_to_send_off = rca_temp.run()

    print('Anomaly Detection Done.')
    if results_to_send_off is None:
        return False
    
    submit(results_to_send_off)
    return True


def process_trace(trace_dict):
    '''
    Process Trace Data
    trace_dict: Dictionary of Traces with format {traceId: LIST OF ELEMENTS OF TRACE}

    Output: List of processed traces
    (processed elapseTime and servicename of CSF into child's cmdbid)
    # CSF's child is always RemoteProcess
    # We dont have actual time column anymore.
    '''
    trace_list = []
    for trace in trace_dict.values():
        child_time = defaultdict(int)
        parent_service = {}
        for element in trace:
            child_time[element['pid']] += int(element['elapsedTime'])
            if element['callType'] == 'RemoteProcess':
                parent_service[element['pid']] = element['cmdb_id']

        # print('filter:', len(filter(lambda x: x['id'] in child_time.keys(), trace)))
        for element in filter(lambda x: x['id'] in child_time, trace):
            # print('elapsedTime, child_time  = ', element['elapsedTime'], child_time[element['id']])
            element['elapsedTime'] = int(element['elapsedTime']) - child_time[element['id']]
            # print('after elapsedTime = ', element['elapsedTime'])
            child_time.pop(element['id'])
            if element['callType'] == 'CSF':
                element['serviceName'] = parent_service.get(element['id'], None)
                parent_service.pop(element['id'], None)

        child_time.pop('None', None)
        if len(child_time) == 0 and len(parent_service) == 0:
            # print('trace = ', trace)
            trace_list.extend(trace)
    # print('trace_list = ', trace_list[:10])
    # print('len=0, tracelist = ', trace_list)
    return trace_list


def rcaprocess():
    '''
    RCA Process
    takes new data then add it into database, remove data more than 20 mins
    this function also calls the anomaly detection function to run anomaly detection
    Wont run detection if last anomalous is within 10 mins

    # IMPORTANT FIXME start detection after 20 mins of starting

    # FIXME get rid dataframe, we can make a dictionary like 
        # { (cmdb_id, serviceName): [List of 20 elements (each element contains 1 min of data)]}

    lock: Thread lock prevent different threads accessing data at the same time

    Output: None
    '''

    global host_df, trace_df, a_time, host_list, trace_dict

    while True:
        st = time.time()
        # try:
        trace = trace_dict.copy()
        host_l = host_list[:]
        host_list = []
        trace_dict = defaultdict(list)
        trace = process_trace(trace)

        timestamp = time.time()*1000

        # print(trace)
        trace_df = trace_df[(trace_df.startTime >= (timestamp-1200000))]
        host_df = host_df[(host_df.timestamp >= (timestamp-1200000))]

        t = time.time()
        t_df = pd.DataFrame(trace)
        h_df = pd.DataFrame(host_l)

        trace_df = pd.concat([trace_df, t_df], axis=0, ignore_index=True)
        host_df = pd.concat([host_df, h_df], axis=0, ignore_index=True)

        trace_df = trace_df[~(trace_df['serviceName'].str.contains('csf', na=True))]

        print('Time to add new data: ', (time.time()-t))

        print('host_df.tail(1) is printed below:')
        print(host_df.tail(1))
        print('trace_df.tail(1) is printed below:')
        print(trace_df.tail(1))

        if (time.time() - a_time) >= 1800:
            tmp_time = time.time()
            result = detection(timestamp)
            if result:
                a_time = tmp_time

        print('Processing + RCA finished in ' + colored('%f', 'cyan') % 
            (time.time() - st) + ' seconds.')

        sleeping_time = 120 - (time.time() - st)
        print('RCA just ran, sleeping for %d seconds' % sleeping_time)
        if sleeping_time > 0:
            time.sleep(sleeping_time)


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
    print('result sent')


host_df = pd.DataFrame(columns=['itemid', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
trace_df = pd.DataFrame(columns=['callType', 'startTime', 'elapsedTime', 'success', 'traceId', 'id', 'pid', 'cmdb_id', 'serviceName'])                                 
a_time = 0.0
host_list = []
trace_dict = defaultdict(list)


def main():
    '''Consume data and react'''
    assert AVAILABLE_TOPICS <= CONSUMER.topics(), 'Please contact admin'

    global host_df, trace_df, a_time, host_list, trace_dict

    host_df = pd.DataFrame(
        columns=['itemid', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
    trace_df = pd.DataFrame(columns=['callType', 'startTime', 'elapsedTime',
                                     'success', 'traceId', 'id', 'pid', 'cmdb_id', 'serviceName'])

    a_time = time.time()
    host_list = []
    trace_dict = defaultdict(list)

    worker = Thread(target=rcaprocess)
    worker.setDaemon(True)
    worker.start()
        
    print('Running under Version 4 of consumer.py')
    print('Started receiving data! Fingers crossed...')
    for message in CONSUMER:
        data = json.loads(message.value.decode('utf8'))

        # Host data
        if message.topic == 'platform-index':
            for item in data['body']['db_oracle_11g']:
                host_list.append(item)

        # ESB data
        elif message.topic == 'business-index':
            continue

        # Trace data
        else:  # message.topic == 'trace'
            if data['callType'] != 'JDBC':
                trace_data = Trace(data)
                trace_dict[trace_data['traceId']].append(trace_data)


if __name__ == '__main__':
    main()

    # '''
    #     Bellow are for testing purposes
    # # '''

    # global host_df, trace_df
    # st = time.time()
    # asdfasdfasdf = time.time()

    # path = r'C:\\Users\\spkgy\\OneDrive\\Documents\\Tsinghua\\Advanced Network Management\\Group Project\\new_test\\'
    # trace_df = pd.read_csv(path + 'trace' + '_530_d2_null.csv')
    # # trace_df = trace_df.drop(['actual_time','path'], axis=1)
    # trace_df = trace_df.sort_values(by=['startTime'], ignore_index=True)
    # # trace = trace[trace.startTime < trace.startTime[0]+1260000]

    # host_df = pd.read_csv(path + 'kpi' + '_530_d2_null.csv')
    # host_df = host_df.sort_values(by=['timestamp'], ignore_index=True)

    # timestamp = int(trace_df['startTime'].iloc[-1])
    # trace_df = trace_df[(trace_df.startTime >= (timestamp-1260000)) & (trace_df.startTime <= timestamp)]
    # trace_df = trace_df[(trace_df.startTime >= (timestamp-1200000)) & (trace_df.startTime <= timestamp)]
    # host_df = host_df[(host_df.timestamp >= (timestamp-1200000))]

    # trace_dict_2 = defaultdict(list)

    # for index, row in trace_df.iterrows():
    #     trace_dict_2[row['traceId']] = trace_dict_2.get(row['traceId'], [])
    #     trace_dict_2[row['traceId']].append(row.to_dict())

    # print('done for loop in %f' % (time.time() - asdfasdfasdf))

    # processed = process_trace(trace_dict_2)
    # t_df = pd.DataFrame(processed)    

    # tttttt = time.time()

    # # trace_df = pd.concat([trace_df, t_df], axis=0, ignore_index=True)
    # # host_df = pd.concat([host_df, h_df], axis=0, ignore_index=True)

    # t_df = t_df[~(t_df['serviceName'].str.contains('csf', na=True))]

    # trace_df = t_df

    # print('Time to add new data: ', (time.time()-tttttt))

    # result = detection(timestamp)

    # print('Processing + RCA finished in ' + colored('%f', 'cyan') % 
    #     (time.time() - st) + ' seconds.')

    # sleeping_time = 30 - (time.time() - st)
    # print('RCA just ran, sleeping for %d seconds' % sleeping_time)
