import numpy as np
import pandas as pd
import itertools

table = pd.DataFrame({'docker_001': {'db_003': np.nan, 'db_007': 4.2877579824499445, 'db_009': 4.013765573490871, 'docker_001': 5.752812717443665, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 3.027980535279805, 'docker_006': 6.243769387100224, 'docker_007': np.nan, 'docker_008': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_002': {'db_003': np.nan, 'db_007': 4.48238973939832, 'db_009': 2.74420759518165, 'docker_001': np.nan, 'docker_002': 4.061037378582316, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 3.542195309656589, 'docker_006': 3.90176257873512, 'docker_007': np.nan, 'docker_008': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_003': {'db_003': np.nan, 'db_007': 6.733153983758381, 'db_009': 11.87775383388791, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 31.090924618537915, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 13.55284558039818, 'docker_008': 24.440113334235257, 'os_021': np.nan, 'os_022': np.nan}, 'docker_004': {'db_003': np.nan, 'db_007': 6.607896945249073, 'db_009': 31.672064681292245, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': 48.155100812281994, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 15.102971390264365, 'docker_008': 34.253714666448516, 'os_021': np.nan, 'os_022': np.nan}, 'docker_005': {'db_003': 16.093142824998633, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 56.67667674456055, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_006': {'db_003': 17.768844664187498, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': 45.371031336975705, 'docker_007': np.nan, 'docker_008': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_007': {'db_003': 12.381311885048907, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 61.241510630164996, 'docker_008': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_008': {'db_003': 17.649008803028753, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': 137.6187518112293, 'os_021': np.nan, 'os_022': np.nan}, 'os_021': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 6.678199760190315, 'docker_004': 30.94269405719789, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'os_021': 25.57740264051314, 'os_022': np.nan}, 'os_022': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': 4.723860208461066, 'docker_002': 5.096864883364287, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'os_021': np.nan, 'os_022': 14.028979254938644}}
)

print(table)

dockers = ['docker_001', 'docker_002', 'docker_003', 'docker_004',
           'docker_005', 'docker_006', 'docker_007', 'docker_008']
docker_hosts = ['os_017', 'os_018', 'os_019', 'os_020']

os_kpi_names = ['Sent_queue', 'Received_queue']
db_kpi_names = ['Proc_User_Used_Pct', 'Proc_Used_Pct',
                'Sess_Connect', 'On_Off_State', 'tnsping_result_time']

docker_lookup_table = {}
for i in range(len(dockers)):
    docker_lookup_table[dockers[i]] = docker_hosts[i % 4]


def find_anomalous_kpi(cmdb_id, local_abnormal):
    # two inputs, cmdb_id and local_abnormal. cmdb_id is the host we sent to server, local_abnormal is a boolean used for 'docker' anomalies.
    kpi_names = []
    if 'os' in cmdb_id:
        # os is always just ['Sent_queue', 'Received_queue']
        kpi_names = os_kpi_names
    elif 'docker' in cmdb_id:
        # if the local method is abnormal, i.e. the self-calling function is abnormal, it is a cpu fault
        if local_abnormal:
            kpi_names = ['container_cpu_used']
        else:
            # if the self calling function is not abnormal, it is a network error
            kpi_names = [None]
            print('Docker network problem')
    else:
        kpi_names = db_kpi_names
        # host_data_subset = host_data.loc[(host_data.cmdb_id == cmdb_id) & (host_data.name.isin(kpi_names))]
        # results_dict = {}
        # for kpi, values in host_data_subset.groupby('name')['value']:
        #     values = list(values)
        #     score =  esd_test(np.array(values), 0.95, 0.1, True)
        #     results_dict[kpi] = score
        # db_connection_limit = [results_dict['Proc_User_Used_Pct'],results_dict['Proc_Used_Pct'],results_dict['Sess_Connect']]
        # db_connection_limit_score = np.mean(db_connection_limit)
        # db_close = [results_dict['On_Off_State'],results_dict['tnsping_result_time']]
        # db_close_score = np.mean(db_close)
        # if db_connection_limit_score > db_close_score:
        #     kpi_names = kpi_names[:3]
        # else:
        #     kpi_names = kpi_names[3:]

    return kpi_names


def localize(dodgy_host_dict, local_abnormal):
    dodgy_hosts = list(dodgy_host_dict.keys())
    n = len(dodgy_host_dict)
    print('We found %d anomalies, printed below:' % n)
    print(dodgy_host_dict)
    if n < 1:
        return None
    if n == 1:
        KPIs = find_anomalous_kpi(
            dodgy_hosts[0], local_abnormal[dodgy_hosts[0]])
        to_be_sent = []
        for KPI in KPIs:
            to_be_sent.append([dodgy_hosts[0], KPI])
        return to_be_sent
    if n >= 2:
        to_be_sent = []

        # create lists containing all the potential os and docker anomalies
        os = [x for x in dodgy_hosts if 'os' in x]
        docker = [y for y in dodgy_hosts if 'docker' in y]

        if len(os) == 2:
            # two os means it must be os_001
            KPIS = find_anomalous_kpi('os_001', False)
            return [['os_001', KPIS[0]], ['os_001', KPIS[1]]]

        if len(docker) >= 2:
            # 2 or more potential docker anomalies, so we check if its a db_003 first
            if len(docker) >= 4:
                if sorted(docker)[-4:] == ['docker_005', 'docker_006', 'docker_007', 'docker_008']:
                    KPIS = find_anomalous_kpi('db_003', False)
                    for kpi in KPIS:
                        to_be_sent.append(['db_003', kpi])
                    return to_be_sent
            # if we reach here, its not a db_003, so it might be an os_00x
            c = list(itertools.combinations(docker, 2))
            print(c)
            for a, b in c:
                if docker_lookup_table[a] == docker_lookup_table[b]:
                    KPIS = find_anomalous_kpi(docker_lookup_table[a], False)
                    for kpi in KPIS:
                        to_be_sent.append([docker_lookup_table[a], kpi])
                    return to_be_sent

        print('The hosts found do not have appear to have common hosts, hence we take the best one.')
        if 'fly' in dodgy_hosts[0]:
            # fly remote means it must be os_009
            KPIs = find_anomalous_kpi('os_009', False)
            for kpi in KPIs:
                to_be_sent.append(['os_009', kpi])
            return to_be_sent
        else:
            # if there are 2 or more potential anomalies and there are no similarities, we simply
            # return the 'most anomalous' one
            KPIs = find_anomalous_kpi(
                dodgy_hosts[0], local_abnormal[dodgy_hosts[0]])
            for kpi in KPIs:
                to_be_sent.append([dodgy_hosts[0], kpi])
            return to_be_sent


def find_anomalous_rows(min_threshold=10):
    # get a threshold. either a qarter of the max value in the entire table or the min_threshold value
    threshold = max(0.25 * table.stack().max(), min_threshold)
    print(threshold)

    local_abnormal = {}
    # these dictionaries store the number of anomalies in the rows/columns of a host respectively
    row_dict = {}
    column_dict = {}
    # these dictionaries store the values of the rows/columns of a host respectively
    row_confidence_dict = {}
    column_confidence_dict = {}

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

    output = localize(dodgy_hosts, local_abnormal)
    print(output)


find_anomalous_rows()
