import glob
import os
import pandas as pd

# path = 'training_data/2020_05_04/host/' # For host folder
path = 'training_data/2020_04_11/host/'

csv_files = glob.glob(path+"*.csv")

dataset = []
for file in csv_files:
    content = pd.read_csv(file)
    content = content[content['timestamp'] < content['timestamp'][0]+100000]
    dataset.append(content)

dataset = pd.concat(dataset, axis=0, ignore_index=True)
# dataset = dataset.sort_values(by=['timestamp']) # For host folder
dataset = dataset.sort_values(by=['timestamp'])

dataset.to_csv(path+"host_data_sample.csv")
