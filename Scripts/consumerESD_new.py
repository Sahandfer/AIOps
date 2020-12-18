#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Example for RCAing like a B0$$
'''
import networkx as nx
import requests
import json
import statistics
# from kafka import KafkaConsumer
import itertools
import operator
import pickle
import matplotlib.pyplot as plt
import time
import numpy as np
import pandas as pd
from scipy.stats import t
from termcolor import colored
from sklearn import preprocessing
from sklearn.cluster import Birch
from sklearn.neighbors import KernelDensity
from sklearn.ensemble import IsolationForest
from threading import Thread, Lock


class ESB_Analyzer():
    def __init__(self, esb_data):
        self.esb_data = esb_data
        self.initialize()

    def initialize(self):
        filename = 'birch_model_avgtime.sav'
        self.avg_time_model = pickle.load(open(filename, 'rb'))
        filename = 'birch_model_success.sav'
        self.succee_rate_model = pickle.load(open(filename, 'rb'))
        print('ESB models loaded')

    def update_esb_data(self, esb_data):
        self.esb_data = esb_data

    def birch(self, values, ctype):  # values should be a list
        X = np.reshape(values, (-1, 1))
        brc = self.avg_time_model if ctype == "time" else self.succee_rate_model
        return brc.predict(X)

    def analyze_esb(self, esb_dict):
        esb_tmp = self.esb_data.append(esb_dict, ignore_index=True)
        values = esb_tmp['avg_time'].tolist()
        # print(values)
        birch_labels_time = self.birch(values,"time")
        # birch_labels_rate = self.birch(self.esb_data['avg_time'])
        for label in birch_labels_time:
            if (label != 0):
                print("Found esb_anomaly in avg_time")
                return True

        values = esb_tmp['succee_rate'].tolist()
        # print(values)
        birch_labels_time = self.birch(values,"rate")
        for label in birch_labels_time:
            if (label != 0):
                print("Found esb_anomaly in success rate")
                return True

        self.update_esb_data(esb_tmp)

        return False


class RCA():
    def __init__(self, trace_data, host_data, alpha=0.95, ub=0.02, take_minute_averages_of_trace_data=True, division_milliseconds=60000):
        self.trace_data = trace_data
        self.host_data = host_data
        self.alpha = alpha
        self.ub = ub
        self.anomaly_chart = None
        self.take_minute_averages_of_trace_data = take_minute_averages_of_trace_data
        self.division_milliseconds = division_milliseconds

    def run(self):
        self.trace_processing()
        
        print('Running RCA on %d trace data rows and %d host data rows' %
              (len(self.trace_data), len(self.host_data)))
        overall_start_time = time.time()

        print('Computing for anomaly score chart...')
        self.hesd_trace_detection(alpha=self.alpha, ub=self.ub)
        print('Score chart computed!')

        self.local_initiate()
        output = self.find_anomalous_rows()

        print('The output to send to the server is: ' +
              colored(str(output), 'magenta'))

        print('RCA finished in ' + colored('%f', 'cyan') %
              (time.time() - overall_start_time) + ' seconds.')
        return output

    def esd_test_statistics(self, x, hybrid=True):
        """
        Compute the location and dispersion sample statistics used to carry out the ESD test.
        """
        if hybrid:
            location = pd.Series(x).median(skipna=True) # Median
            dispersion = np.median(np.abs(x - np.median(x))) # Median Absolute Deviation
        else:  
            location = pd.Series(x).mean(skipna=True) # Mean
            dispersion = pd.Series(x).std(skipna=True) # Standard Deviation
            
        return location, dispersion

    def esd_test(self, x, alpha=0.95, ub=0.499, hybrid=True):
        """
        Carries out the Extreme Studentized Deviate(ESD) test which can be used to detect one or more outliers present in the timeseries
        
        x      : List, array, or series containing the time series
        freq   : Int that gives the number of periods per cycle (7 for week, 12 for monthly, etc)
        alpha  : Confidence level in detecting outliers
        ub     : Upper bound on the fraction of datapoints which can be labeled as outliers (<=0.499)
        hybrid : Whether to use the robust statistics (median, median absolute error) or the non-robust versions (mean, standard deviation) to test for anomalies
        """
        nobs = len(x)
        if ub > 0.4999:
            ub = 0.499
        k = max(int(np.floor(ub * nobs)), 1) # Maximum number of anomalies. At least 1 anomaly must be tested.
            
        # Carry out the esd test k times  
        res = np.ma.array(x, mask=False) # The "ma" structure allows masking of values to exclude the elements from any calculation
        anomalies = [] # returns the indices of the found anomalies
        med = np.median(x)
        for i in range(1, k+1):
            location, dispersion = self.esd_test_statistics(res, hybrid) # Sample statistics
            tmp = np.abs(res - location) / dispersion
            idx = np.argmax(tmp) # Index of the test statistic
            test_statistic = tmp[idx]
            n = nobs - res.mask.sum() # sums  nonmasked values
            critical_value = (n - i) * t.ppf(alpha, n - i - 1) / np.sqrt((n - i - 1 + np.power(t.ppf(alpha, n - i - 1), 2)) * (n - i - 1)) 
            if test_statistic > critical_value:
                anomalies.append((x[idx]-med) / med)
                # anomalies.append(test_statistic)
            res.mask[idx] = True
        if len(anomalies) == 0:
            return 0
        return np.mean(np.abs(anomalies))
    
    def hesd_trace_detection(self, alpha=0.95, ub=0.02):
        grouped_df = self.trace_data.groupby(['cmdb_id', 'serviceName'])[['startTime','actual_time']]

        self.anomaly_chart = pd.DataFrame()
        for (a, b), value in grouped_df:
            value['time_group'] = value.startTime//self.division_milliseconds
            value = value.groupby(['time_group'])['actual_time'].mean().reset_index()
            result = self.esd_test(value['actual_time'].to_numpy(), alpha=alpha, ub=ub, hybrid=True)
            self.anomaly_chart.loc[b,a] = result

        self.anomaly_chart = self.anomaly_chart.sort_index()
        print(self.anomaly_chart)

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
        self.dockers = ['docker_001', 'docker_002', 'docker_003', 'docker_004',
                'docker_005', 'docker_006', 'docker_007', 'docker_008']
        self.docker_hosts = ['os_017', 'os_018', 'os_019', 'os_020']

        self.docker_kpi_names = ['container_cpu_used', None]
        self.os_kpi_names = ['Sent_queue', 'Received_queue']
        self.db_kpi_names = ['Proc_User_Used_Pct','Proc_Used_Pct','Sess_Connect','On_Off_State', 'tnsping_result_time']

        self.docker_lookup_table = {}
        for i in range(len(self.dockers)):
            self.docker_lookup_table[self.dockers[i]] = self.docker_hosts[i % 4]

    def isolation_forest(self, values):
        X = np.reshape(values, (-1, 1))
        iso = IsolationForest()
        yhat = iso.fit_predict(X)
        return yhat
    
    def find_threshold(self, table):
        thresh_idx = 0
        seen_normal = False # If seen 1 or not
        values = table.stack().tolist()
        yhat = self.isolation_forest(values)
        for i in range(len(yhat)):
            if (yhat[i] == 1):
                seen_normal = True
            elif (yhat[i] == -1) and seen_normal:
                thresh_idx = i
                break
            
        secondary = values[thresh_idx:-1]
        med = statistics.median(secondary)
        threshold = max(2,statistics.median([abs(x-med) for x in secondary]))
        print(threshold)
        
        return threshold

    def find_anomalous_rows(self):
        table = self.anomaly_chart.copy()
        threshold = self.find_threshold(table)
        
        row_col_dict = {}
        column_dict= {}
        row_dict = {}
        confidence_col = {}
        confidence_row = {}

        for column in table:
            for index, row in table.iterrows():
                if (str(row[column]) != 'nan'):
                    increment = 1 if (row[column] >= threshold) else 0
                    if (column in column_dict.keys()):
                        column_dict[column] += increment
                        confidence_col[column].append(row[column])
                    else:
                        column_dict[column] = increment
                        confidence_col[column] = [row[column]]

                    if (index in row_dict.keys()):
                        row_dict[index] += increment
                        confidence_row[index].append(row[column])
                    else:
                        row_dict[index] = increment
                        confidence_row[index] = [row[column]]

                    if (index == column) and increment == 1:
                        row_col_dict[index] = True
                    else:
                        if index not in row_col_dict.keys():
                            row_col_dict[index] = False
        
        for key, value in confidence_col.items():
            confidence_col[key] = statistics.mean(value)
            
        for key, value in confidence_row.items():
            confidence_row[key] = statistics.mean(value)
            
            
        final_dict = {}
        for key in row_dict.keys():
            if (key in column_dict.keys()):
                row_dict[key] = (row_dict[key]+ column_dict[key]) //2
                confidence_row[key] = (confidence_row[key] + confidence_col[key]) //2
            final_dict[key] = row_dict[key] * confidence_row[key]
        
        output = []
        final_rows =  {v:k for k, v in sorted(final_dict.items(), key=operator.itemgetter(1),reverse=True)}
        vals = list(final_rows.keys())
        if len(vals) > 1:
            med = statistics.mean(vals)
            yhat = self.isolation_forest(vals)
            for i in range(len(yhat)):
                if (yhat[i] == -1) and (vals[i]>=med):
                    output.append(final_rows[vals[i]])
        else:
            output.append(final_rows[vals[0]])
        
        final_output = self.localize(output, row_col_dict)
        return final_output


    def find_anomalous_kpi(self, cmdb_id, row_col_same = False):
        kpi_names = []
        if 'os' in cmdb_id:
            kpi_names = self.os_kpi_names
        elif 'docker' in cmdb_id:
            kpi_names = [self.docker_kpi_names[0]] if row_col_same else [self.docker_kpi_names[1]]
        else:
            kpi_names = self.db_kpi_names
            host_data_subset = self.host_data.loc[(self.host_data.cmdb_id == cmdb_id) & (self.host_data.name.isin(kpi_names))]
            results_dict = {}
            for kpi, values in host_data_subset.groupby('name')['value']:
                values = list(values)
                score =  self.esd_test(np.array(values), 0.95, 0.1, True)
                results_dict[kpi] = score
            db_connection_limit = [results_dict['Proc_User_Used_Pct'],results_dict['Proc_Used_Pct'],results_dict['Sess_Connect']]
            db_connection_limit_score = np.mean(db_connection_limit)
            db_close = [results_dict['On_Off_State'],results_dict['tnsping_result_time']]
            db_close_score = np.mean(db_close)
            if db_connection_limit_score > db_close_score:
                kpi_names = kpi_names[:3]
            else:
                kpi_names = kpi_names[3:]             

        return kpi_names


    def localize(self, output, row_col_dict):
        print(output)
        n = len(output)
        print('%d anomalies found' % n)
        if n < 1:
            return None
        if n == 1:
            KPIs = self.find_anomalous_kpi(output[0], row_col_dict[output[0]])
            to_be_sent = []
            for KPI in KPIs:
                to_be_sent.append([output[0], KPI])
            return to_be_sent
        if n >= 2:
            to_be_sent = []
            os = [x for x in output if 'os' in x]
            docker = [y for y in output if 'docker' in y]

            if len(os)==2:
                KPIS = self.find_anomalous_kpi('os_001')
                return [['os_001', KPIS[0]], ['os_001', KPIS[1]]]

            if len(docker) >= 2:
                c = list(itertools.combinations(docker,2))
                for a, b in c:
                    if self.docker_lookup_table[a] == self.docker_lookup_table[b]:
                        KPIS = self.find_anomalous_kpi(self.docker_lookup_table[a])
                        for kpi in KPIS:
                            to_be_sent.append([self.docker_lookup_table[a], kpi])
                        return to_be_sent

            KPIs = self.find_anomalous_kpi(output[0], row_col_dict[output[0]])
            for kpi in KPIs:
                to_be_sent.append([output[0], kpi])
            return to_be_sent


    def update_trace_data(self, trace_data):
        self.trace_data = trace_data

    def update_host_data(self, host_data):
        self.host_data = host_data

    def trace_processing(self):
        print("Started trace processing")
        p_time = time.time()
        self.trace_data = self.trace_data[self.trace_data['callType'] != 'FlyRemote'].copy()

        df1 = self.trace_data[self.trace_data['callType']=='RemoteProcess'][['pid','cmdb_id']]
        df1 = df1.set_index('pid')

        elapse_time = {}
        children = {}

        def change_csf_and_setup_dicts(row):
            if row['id'] in df1.index:
                row['serviceName'] = df1.at[row['id'],'cmdb_id']
            if row['pid'] != 'None':
                children[row['pid']] = children.get(row['pid'], [])
                children[row['pid']].append(row['id'])
            elapse_time[row['id']] = float(row['elapsedTime'])
            return row
        self.trace_data = self.trace_data.apply(change_csf_and_setup_dicts, axis=1)

        self.trace_data['actual_time'] = 0.0
        def get_actual_time(row):
            total_child = 0.0
            if row['id'] in children:
                for child in children[row['id']]:
                    total_child += elapse_time[child]
            row['actual_time'] = row['elapsedTime'] - total_child
            return row        
        self.trace_data = self.trace_data.apply(get_actual_time, axis = 1)
        
        self.trace_data = self.trace_data[~(self.trace_data['serviceName'].str.contains('csf', na=True))]

        print("Trace processed in ", time.time()-p_time, 'seconds')
        # print(self.trace_data)


# Three topics are available: platform-index, business-index, trace.
# Subscribe at least one of them.
AVAILABLE_TOPICS = set(['platform-index', 'business-index', 'trace'])
# CONSUMER = KafkaConsumer('platform-index', 'business-index', 'trace',
#                          bootstrap_servers=['172.21.0.8', ],
#                          auto_offset_reset='latest',
#                          enable_auto_commit=False,
#                          security_protocol='PLAINTEXT')


class Trace():  # pylint: disable=invalid-name,too-many-instance-attributes,too-few-public-methods
    '''Structure for traces'''

    __slots__ = ['call_type', 'start_time', 'elapsed_time', 'success',
                 'trace_id', 'id', 'pid', 'cmdb_id', 'service_name', 'ds_name']

    def __new__(self, data):
        self.trace = data
        if self.trace['callType'] == 'JDBC' or self.trace['callType']=='LOCAL':
            try:
                self.trace['serviceName'] = data['dsName']
            except:
                print(data)
                print('JDBC doesnt have dsName')
        
        elif self.trace['callType']=='RemoteProcess' or self.trace['callType']=='OSB':
            self.trace['serviceName'] = data['cmdb_id']

        if 'dsName' in self.trace:
            self.trace.pop('dsName')

        return self.trace


def detection(timestamp):
    global host_df, trace_df
    print('Starting Anomaly Detection')
    startTime = timestamp - 1200000  # one minute before anomaly

    # print(len(trace_df), trace_df.head())
    # print(len(host_df), host_df.head())
    trace_df_temp = trace_df[(trace_df['startTime'] >= startTime) &
                             (trace_df['startTime'] <= timestamp)]
    host_df_temp = host_df[(host_df['timestamp'] >= startTime) &
                           (host_df['timestamp'] <= timestamp)]
    # print(len(trace_df_temp), trace_df_temp.head())
    # print(len(host_df_temp), host_df_temp.head())
    if (len(trace_df_temp) == 0) or (len(host_df_temp) == 0):
        print('Error: empty dataframe')

    rca_temp = RCA(trace_data=trace_df_temp, host_data=host_df_temp)
    results_to_send_off = rca_temp.run()

    print('Anomaly Detection Done.')
    if results_to_send_off is None:
        # print('Nothing detected')
        return False
    # for a in anom_hosts:
    #     item = a.split(':')[0]
    #     if (item not in anoms):
    #         anoms.append(item)
    # print(results_to_send_off)
    # submit(results_to_send_off)
    return True


def rcaprocess(esb_item, trace, host, timestamp, lock):
    global host_df, trace_df, esb_anal, a_time
    esb_anomaly = False

    # print(trace)
    trace_df = trace_df[(trace_df.startTime >= (timestamp-1260000))]
    host_df = host_df[(host_df.timestamp >= (timestamp-1260000))]
    
    t = time.time()
    t_df = pd.DataFrame(trace)
    h_df = pd.DataFrame(host)

    trace_df = pd.concat([trace_df, t_df], axis=0, ignore_index=True)
    host_df = pd.concat([host_df, h_df], axis=0, ignore_index=True)

    print('Time to add new data: ', (time.time()-t))

    print(esb_anal.esb_data.tail(1))
    print(host_df.tail(1))
    print(trace_df.tail(1))

    with lock:
        esb_anomaly = esb_anal.analyze_esb(esb_item)
        if (time.time() - a_time) >= 600 and esb_anomaly:
            tmp_time = time.time()
            print("oops")
            # detection(timestamp)
            result = detection(timestamp)
            print('Anomaly at: ', timestamp)
            if result:
                a_time = tmp_time


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


# esb_df = pd.DataFrame(columns=[
#                       'serviceName', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate'])
# host_df = pd.DataFrame(
#     columns=['itemid', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
# trace_df = pd.DataFrame(columns=['callType', 'startTime', 'elapsedTime',
#                                  'success', 'traceId', 'id', 'pid', 'cmdb_id', 'serviceName'])
# esb_anal = ESB_Analyzer(esb_df)
# a_time = 0.0


def main():
    '''Consume data and react'''
    assert AVAILABLE_TOPICS <= CONSUMER.topics(), 'Please contact admin'

    global esb_df, host_df, trace_df, esb_anal, a_time

    esb_df = pd.DataFrame(columns=[
                          'serviceName', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate'])
    host_df = pd.DataFrame(
        columns=['itemid', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
    trace_df = pd.DataFrame(columns=['callType', 'startTime', 'elapsedTime',
                                     'success', 'traceId', 'id', 'pid', 'cmdb_id', 'serviceName'])
    esb_anal = ESB_Analyzer(esb_df)
    a_time = 0.0

    lock = Lock()

    print('Started receiving data! Fingers crossed...')

    # Dataframes for the three different datasets

    trace_list = []
    host_list = []

    for message in CONSUMER:
        data = json.loads(message.value.decode('utf8'))

        # Host data
        if message.topic == 'platform-index':
            for stack in data['body']:
                for item in data['body'][stack]:
                    # host_df = host_df.append(item, ignore_index=True)
                    host_list.append(item)

        # ESB data
        elif message.topic == 'business-index':
            timestamp = data['startTime']

            esb_item = data['body']['esb'][0]

            try:
                Thread(target=rcaprocess, args=(esb_item, trace_list, host_list, timestamp, lock)).start()
            except:
                print("Error: unable to start rcaprocess")

            trace_list = []
            host_list = []

        # Trace data
        else:  # message.topic == 'trace'
            # print(data)
            trace_list.append(Trace(data))

if __name__ == '__main__':
    # main()

    '''
        Bellow are for testing purposes
    # '''
    
    global host_df, trace_df

    # path= 'training_data/'
    path = r'C:\\Users\spkgy\\OneDrive\\Documents\\Tsinghua\\Advanced Network Management\\Group Project\\'
    trace_df = pd.read_csv(path+'trace_527323_docker001.csv')
    # trace_df = trace_df.drop(['actual_time','path'], axis=1)
    # trace_df = trace_df.drop(['path'], axis=1)
    trace_df = trace_df.sort_values(by=['startTime'], ignore_index=True)
    # trace = trace[trace.startTime < trace.startTime[0]+1260000]

    host_df = pd.read_csv(path+'kpi_data_527_docker001.csv')
    host_df = host_df.sort_values(by=['timestamp'], ignore_index=True)

    # print(trace_df)
    # print(host_df)
    timestamp = int(trace_df['startTime'].iloc[-1]-180000)
    
    # print(timestamp)
    # print(timestamp)
    # trace_df = trace_df[(trace_df.startTime >= (timestamp-1260000))]
    # print(host_df)
    detection(timestamp)
