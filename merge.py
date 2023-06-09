import pandas as pd
import  utils

# 按行合并多个Dataframe数据
def mergeData():
    monday = utils.writeData("MachineLearningCVE\Monday-WorkingHours.pcap_ISCX.csv")

    # 剔除第一行属性特征名称
    monday = monday.drop([0])
    friday1 = utils.writeData("MachineLearningCVE\Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv")
    friday1 = friday1.drop([0])
    friday2 = utils.writeData("MachineLearningCVE\Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv")
    friday2 = friday2.drop([0])
    friday3 = utils.writeData("MachineLearningCVE\Friday-WorkingHours-Morning.pcap_ISCX.csv")
    friday3 = friday3.drop([0])
    thursday1 = utils.writeData("MachineLearningCVE\Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv")
    thursday1 = thursday1.drop([0])
    thursday2 = utils.writeData("MachineLearningCVE\Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv")
    thursday2 = thursday2.drop([0])
    tuesday = utils.writeData("MachineLearningCVE\Tuesday-WorkingHours.pcap_ISCX.csv")
    tuesday = tuesday.drop([0])
    wednesday = utils.writeData("MachineLearningCVE\Wednesday-workingHours.pcap_ISCX.csv")
    wednesday = wednesday.drop([0])
    frame = [monday, friday1, friday2, friday3, thursday1, thursday2, tuesday, wednesday]

    # 合并数据
    result = pd.concat(frame)
    list = clearDirtyData(result)
    result = result.drop(list)
    return result


# 清除CIC-IDS数据集中的脏数据，第一行特征名称和含有Nan、Infiniti等数据的行数
def clearDirtyData(df):
    dropList = df[(df[14] == "Nan") | (df[15] == "Infinity")].index.tolist()
    return dropList


raw_data = mergeData()
last_column_index = raw_data.shape[1] - 1
print(raw_data[last_column_index].value_counts())
file = 'data/total.csv'
raw_data.to_csv(file, index=False, header=False)