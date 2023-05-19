
import numpy as np
import pandas as pd
import csv
global label_list  # label_list为全局变量
#在编写程序的时候，如果想为一个在函数外的变量重新赋值，并且这个变量会作用于许多函数中时，就需要告诉python这个变量的作用域是全局变量。此时用global语句就可以变成这个任务，也就是说没有用global语句的情况下，是不能修改全局变量的。
# 定义数据预处理函数
def preHandel_data():
    source_file = 'data\Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv'  # 源文件
    handled_file ='data\Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv'  # 处理后的文件
    data_file = open(handled_file, 'w', newline='')  # python中添加newline=''这一参数使写入的文件没有多余的空行
    # W 打开一个文件只用于写入。。如果该文件不存在，创建新文件。
    # r 以只读方式打开文件。文件的指针将会放在文件的开头。这是默认模式。
    with open(source_file, 'r') as data_source:  # 只读方式打开源文件
        csv_reader = csv.reader(data_source)  # 读取源文件
        # 返回一个reader对象，该对象将遍历csv文件中的行。从csv文件中读取的每一行都作为字符串列表返回。
        csv_writer = csv.writer(data_file)  # 写入文件
        count = 0  # 记录数据的行数，初始化为0
        for row in csv_reader:  # 读取csv中每一行为一个list
            temp_line = np.array(row)  # 将每行数据存入temp_line数组里
            # temp_line[1] = handleProtocol(row)  # 数组的第一列为handprotocal函数处理后的值
            # temp_line[2] = handleService(row)
            # temp_line[3] = handleFlag(row)
            temp_line[78] = handleLabel(row)
            csv_writer.writerow(temp_line)  # 将转换后的文件存入csv_write中
            count += 1
            # 输出每行数据中所修改后的状态
            # print(count, 'status:', temp_line[1], temp_line[2], temp_line[3], temp_line[78])
            print(count, 'status:', temp_line[78])
        data_file.close()  # 关闭处理后的文件
def find_index(x, y): #x表示数组元素，y表示数组元素的索引
    return [i for i in range(len(y)) if y[i] == x] #从0行开始遍历到结束行，如果这一列里面有对对应的字符串，返回对应的数字表示
# 定义将源文件行中攻击类型转换成数字标识的函数(训练集中共出现了22个攻击类型，而剩下的17种只在测试集中出现)
def handleLabel(input):
    global label_list  # 在函数内部使用全局变量并修改它
    if input[78] in label_list:
        return find_index(input[78], label_list)[0]
    else:
        label_list.append(input[78])
        return find_index(input[78], label_list)[0]
if __name__ == '__main__':
    global label_list  # 声明一个全局变量的列表并初始化为空
    label_list = []
    preHandel_data()

