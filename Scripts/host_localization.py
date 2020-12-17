import numpy as np
import pandas as pd
from sklearn import preprocessing
from sklearn.cluster import Birch

dockers = ['docker_001', 'docker_002', 'docker_003', 'docker_004',
           'docker_005', 'docker_006', 'docker_007', 'docker_008']
docker_hosts = ['os_017', 'os_018', 'os_019', 'os_020']

docker_kpi_names = ['container_cpu_used', None]
os_kpi_names = ['Sent_queue', 'Received_queue']
db_kpi_names = ['Proc_User_Used_Pct','Proc_Used_Pct','Sess_Connect','On_Off_State', 'tnsping_result_time']

docker_lookup_table = {}

host_data = pd.read_csv('kpi_data.csv')

for i in range(len(dockers)):
    docker_lookup_table[dockers[i]] = docker_hosts[i % 4]


def find_anomalous_rows(table, min_threshold = 5):
    threshold = max( 0.5 * table.stack().max(), min_threshold)
    dodgy_rows = []
    just_rows = []
    for column in table:
        v = 0
        r = ''
        for index, row in table.iterrows():
            if (row[column] > threshold):
                if index == column:
                    dodgy_rows.append([index, row[column]])
                    just_rows.append(index)
                    break
                elif (row[column] > v):
                    v = row[column]
                    r = index
        if r != '':
            dodgy_rows.append([r, column, v])
            just_rows.append(r)

    return dodgy_rows, list(set(just_rows))


def do_birch(values, threshold):
    values = np.array(values)
    normalized_time = preprocessing.normalize([np.array(values)]).reshape(-1,1)
    birch = Birch(branching_factor=50, n_clusters=None, threshold=threshold, compute_labels=True)
    birch.fit_predict(normalized_time)
    return (np.unique(birch.labels_).size>1), birch.labels_


def find_anomalous_kpi(cmdb_id):
    kpi_names = []
    if 'os' in cmdb_id:
        kpi_names = os_kpi_names
    elif 'docker' in cmdb_id:
        kpi_names = docker_kpi_names
    else:
        kpi_names = db_kpi_names

    return kpi_names

    # host_data_subset = host_data[host_data['cmdb_id']==cmdb_id][['name', 'value']]
    # KPI = 'fakin nooothan'
    # max_so_far = 0
    # for _kpi, values in host_data_subset.groupby('name')['value']:
    #     if _kpi in kpi_names:
    #         values = list(values)
    #         is_anomalous, labels = do_birch(values, 0.02)
    #         if is_anomalous:
    #             score = sum(labels!=0)/len(labels)
    #             if score > max_so_far:
    #                 max_so_far = score
    #                 KPI = _kpi

    # if KPI == 'fakin nooothan':
    #     return None
    # elif KPI == 'container_cpu_used':
    #     return KPI 
    # elif KPI in ['Sent_queue', 'Received_queue']:
    #     return 'Sent_queue;Received_queue'
    # elif KPI in ['Proc_User_Used_Pct','Proc_Used_Pct','Sess_Connect']:
    #     return 'Proc_User_Used_Pct;Proc_Used_Pct;Sess_Connect'
    # elif KPI in ['On_Off_State', 'tnsping_result_time']:
    #     return 'On_Off_State;tnsping_result_time'


def localize(dodgy_rows, just_rows):
    n = len(just_rows)
    if n < 1:
        return [[]]
    if n == 1:
        KPIs = find_anomalous_kpi(just_rows[0])
        to_be_sent = []
        for KPI in KPIs:
            to_be_sent.append([just_rows[0], KPI])
        return to_be_sent
    if n ==2:
        r0 = just_rows[0]
        r1 = just_rows[1]
        if ('os' in r0) and ('os' in r1):
            KPI = find_anomalous_kpi('os_001')
            return [['os_001', KPI]]
        elif ('docker' in r0) and ('docker' in r1):
            if docker_lookup_table[r0] == docker_lookup_table[r1]:
                KPI = find_anomalous_kpi(docker_lookup_table[r0])
                return [[docker_lookup_table[r0], KPI]]
        else:
            KPI0s = find_anomalous_kpi(r0)
            KPI1s = find_anomalous_kpi(r1)
            to_be_sent = []
            for kpi in KPI0s:
                to_be_sent.append([r0, kpi])
            for kpi in KPI1s:
                to_be_sent.append([r1, kpi])
            return to_be_sent
    if n > 2:
        dodgy_rows.sort(key = lambda x: x[2], reverse = True)
        just_rows = [x[0] for x in dodgy_rows]
        just_rows = list(set(just_rows))
        return localize(dodgy_rows[:2], just_rows[:2])


