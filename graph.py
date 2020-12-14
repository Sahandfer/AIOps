import os
import pandas as pd
import numpy as np
import networkx as nx
import csv
import matplotlib
import matplotlib.pyplot as plt


path = os.path.dirname(os.path.abspath(__file__))
data = pd.read_csv(path + '\\training_data\\2020_05_04\\trace\\trace_data_sample.csv', index_col=False)
data.drop('Unnamed: 0', axis=1, inplace = True)

print(data.cmdb_id.dropna().unique())

data['serviceName'] = data['serviceName'].mask(pd.isnull, data['dsName'])
data['host_service'] = data['cmdb_id'] +':'+ data['serviceName']

data=data[900000:1000000]

names = data.host_service.dropna().unique()
names.sort()

options_final = {}

DG = nx.DiGraph()

for service in names:
    pid_options = []
    filtered = data[data.host_service==service]
    pids = filtered.pid.dropna().unique()
    pids = pids[pids!='None']
    for pid in pids:
        selected = data[data.id==pid]
        if not selected.empty:
            pid_options.append(selected.host_service.iloc[0])
    pid_options = list(set(pid_options))
    pid_options.sort()
    print('destination: %s, sources: %s' %(service,pid_options))
    options_final[service]=(pid_options)

    for source in pid_options:
        DG.add_edge(source, service)



options_final = dict(sorted(options_final.items()))

with open(path + '\\graph_edges.csv', 'w') as f:  
    w = csv.DictWriter(f, options_final.keys())
    w.writeheader()
    w.writerow(options_final)


nx.draw_random(DG,with_labels=True, node_size=700,font_size =6)
plt.show()