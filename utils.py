import pandas as pd

def clearDirtyData(df):
    dropList = df[(df[14]=="Nan")|(df[15]=="Infinity")].index.tolist()
    return dropList

# 根据file读取数据
def writeData(file):
    print("Loading raw data...")
    raw_data = pd.read_csv(file, header=None, low_memory=False)
    return raw_data


# 将大的数据集根据标签特征分为15类，存储到lists集合中
def separateData(raw_data):
    # dataframe数据转换为多维数组
    lists = raw_data.values.tolist()
    temp_lists = []

    # 生成15个空的list集合，用来暂存生成的15种特征集
    for i in range(0, 4):
        temp_lists.append([])

    # 得到raw_data的数据标签集合
    label_set = lookData(raw_data)

    # 将无序的数据标签集合转换为有序的list
    label_list = list(label_set)

    for i in range(0, len(lists)):
        # 得到所属标签的索引号
        data_index = label_list.index(lists[i][len(lists[0]) - 1])
        print(data_index)
        temp_lists[data_index].append(lists[i])
        if i % 5000 == 0:
            print(i)
    saveData(temp_lists, 'data/expendData/',raw_data)
    return temp_lists


# 将lists分批保存到file文件路径下
def saveData(lists, file,raw_data):
    label_set = lookData(raw_data)
    label_list = list(label_set)
    for i in range(0, len(lists)):
        save = pd.DataFrame(lists[i])
        file1 = file + label_list[i] + '.csv'
        save.to_csv(file1, index=False, header=False)


def lookData(raw_data):
    # 打印数据集的标签数据数量
    last_column_index = raw_data.shape[1] - 1
    print(raw_data[last_column_index].value_counts())

    # 取出数据集标签部分
    labels = raw_data.iloc[:, raw_data.shape[1] - 1:]

    # 多维数组转为以为数组
    labels = labels.values.ravel()
    label_set = set(labels)
    return label_set


# lists存储着15类数据集，将数据集数量少的扩充到至少不少于5000条，然后存储起来。
def expendData(lists,raw_data):
    totall_list = []
    for i in range(0, len(lists)):
        while len(lists[i]) < 3000:
            lists[i].extend(lists[i])
        print(i)
        totall_list.extend(lists[i])
    saveData(lists, 'data/expendData/',raw_data)
    save = pd.DataFrame(totall_list)
    file = 'data/expendData/totall_extend.csv'
    save.to_csv(file, index=False, header=False)