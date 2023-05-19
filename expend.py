import pandas as pd
import utils


file = 'data/Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv'
raw_data = utils.writeData(file)
last_column_index = raw_data.shape[1] - 1
print(raw_data[last_column_index].value_counts())
lists = utils.separateData(raw_data)
utils.expendData(lists,raw_data)