'''
Example for data consuming.
'''
import requests
import json

from kafka import KafkaConsumer


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
        if self.trace['callType']=='LOCAL':
            try:
                self.trace['serviceName'] = data['dsName']
            except:
                print(data)
                print('LOCAL doesnt have dsName')
        
        elif self.trace['callType']=='RemoteProcess' or self.trace['callType']=='OSB':
            self.trace['serviceName'] = data['cmdb_id']

        if 'dsName' in self.trace:
            self.trace.pop('dsName')

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


esb_df = pd.DataFrame(columns=[
                      'serviceName', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate'])
host_df = pd.DataFrame(
    columns=['itemid', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
trace_df = pd.DataFrame(columns=['callType', 'startTime', 'elapsedTime',
                                 'success', 'traceId', 'id', 'pid', 'cmdb_id', 'serviceName'])
esb_anal = ESB_Analyzer(esb_df)
a_time = 0.0


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

    print('Running under Sami\'s update 3')

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
    main()