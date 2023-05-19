import pandas as pd
import numpy as np
from sklearn import preprocessing
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import confusion_matrix, zero_one_loss
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import ConfusionMatrix
from tuning import tuning_hyper,martix_search


# 加载数据
raw_data_filename = "data/expendData/totall_extend.csv"
print("Loading raw data...")
raw_data = pd.read_csv(raw_data_filename, header=None, low_memory=False)




# 随机抽取比例，当数据集比较大的时候，可以采用这个，可选项
#raw_data = raw_data.sample(frac=0.03)

# 查看标签数据情况
last_column_index = raw_data.shape[1] - 1
print("print data labels:")
print(raw_data[last_column_index].value_counts())

# 将非数值型的数据转换为数值型数据
# print("Transforming data...")
raw_data[last_column_index], attacks = pd.factorize(raw_data[last_column_index], sort=True)

# 对原始数据进行切片，分离出特征和标签，第1~78列是特征，第79列是标签
features = raw_data.iloc[:, :raw_data.shape[1] - 1]  # pandas中的iloc切片是完全基于位置的索引
labels = raw_data.iloc[:, raw_data.shape[1] - 1:]

# 特征数据标准化，这一步是可选项
features = preprocessing.scale(features)
features = pd.DataFrame(features)

# 将多维的标签转为一维的数组
labels = labels.values.ravel()
print(labels)
# 将数据分为训练集和测试集,并打印维数
df = pd.DataFrame(features)
X_train, X_test, y_train, y_test = train_test_split(df, labels, train_size=0.8, test_size=0.2, stratify=labels)

# print("X_train,y_train:", X_train.shape, y_train.shape)
# print("X_test,y_test:", X_test.shape, y_test.shape)

# 训练模型
print("Training model...")
clf = DecisionTreeClassifier(criterion='entropy', max_depth=32, min_samples_leaf=1, splitter="best")
trained_model = clf.fit(X_train, y_train)
print("Score:", trained_model.score(X_train, y_train))

# 预测
print("Predicting...")
y_pred = clf.predict(X_test)
print("Computing performance metrics...")
results = confusion_matrix(y_test, y_pred)
error = zero_one_loss(y_test, y_pred)

# 根据混淆矩阵求预测精度
list_diag = np.diag(results)
list_raw_sum = np.sum(results, axis=1)
print("Predict accuracy of the decisionTree: ", np.mean(list_diag) / np.mean(list_raw_sum))

ConfusionMatrix.plotMatrix(attacks, y_test, y_pred)

tuning_hyper(X_train,y_train,X_test,y_test,40)

martix_search(X_train,y_train)
