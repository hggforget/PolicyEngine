import pandas as pd
import utils


file_path = 'MachineLearningCVE/Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv'

raw_data=pd.read_csv(file_path, header=None, low_memory=False)
raw_data = raw_data.drop([0])
list = utils.clearDirtyData(raw_data)
raw_data = raw_data.drop(list)
# 得到标签列索引
last_column_index = raw_data.shape[1] - 1
print(raw_data[last_column_index].value_counts())

file = 'data/Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv'
raw_data.to_csv(file, index=False, header=False)