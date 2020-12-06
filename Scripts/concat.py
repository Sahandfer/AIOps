
import pandas as pd

path = 'training_data/2020_05_04/trace/'
a = pd.read_csv(path+'trace_csf.csv')
b = pd.read_csv(path+'trace_fly_remote.csv')
c = pd.read_csv(path+'trace_jdbc.csv')
d = pd.read_csv(path+'trace_local.csv')
e = pd.read_csv(path+'trace_osb.csv')
f = pd.read_csv(path+'trace_remote_process.csv')
# print(len(a)+len(b)+len(c)+len(d)+len(e)+len(f))

# path = "sample/"
# a = pd.read_csv(path+'CSF.csv')
# b = pd.read_csv(path+'FlyRemote.csv')
# c = pd.read_csv(path+'JDBC.csv')
# d = pd.read_csv(path+'LOCAL.csv')
# e = pd.read_csv(path+'OSB.csv')
# f = pd.read_csv(path+'RemoteProcess.csv')

data = [a,b,c,d,e,f]
data = pd.concat(data)
data = data.sort_values(by=['startTime'])
# print(len(data))
data = data[:200000]
data.to_csv(path+'trace_data_sample.csv')

# data = pd.read_csv(path+'training_data/2020_05_04/trace/trace_data_sample.csv')

# data['timeGroup'] = (data.startTime - data.startTime.iloc[0]) // 60000
# for i in range(75):
#     print(sum(data.timeGroup==i))