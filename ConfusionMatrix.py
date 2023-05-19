import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import confusion_matrix


class PlotConfusionMatrix:
    def plot_confusion_matrix(self, labels, cm, title='Confusion Matrix', cmap=plt.cm.binary):
        plt.imshow(cm, interpolation='nearest', cmap=cmap)
        plt.title(title)
        plt.colorbar()
        xlocations = np.array(range(len(labels)))
        plt.xticks(xlocations, labels, rotation=90)
        plt.yticks(xlocations, labels)
        plt.ylabel('True label')
        plt.xlabel('Predicted label')

    def prepareWork(self, labels, y_true, y_pred):
        tick_marks = np.array(range(len(labels))) + 0.5
        cm = confusion_matrix(y_true, y_pred)
        np.set_printoptions(precision=2)
        cm_normalized = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        plt.figure(figsize=(12, 8), dpi=120)

        ind_array = np.arange(len(labels))
        x, y = np.meshgrid(ind_array, ind_array)

        for x_val, y_val in zip(x.flatten(), y.flatten()):
            c = cm_normalized[y_val][x_val]
            if c > 0.01:
                plt.text(x_val, y_val, "%0.2f" % (c,), color='red', fontsize=7, va='center', ha='center')
        # offset the tick
        plt.gca().set_xticks(tick_marks, minor=True)
        plt.gca().set_yticks(tick_marks, minor=True)
        plt.gca().xaxis.set_ticks_position('none')
        plt.gca().yaxis.set_ticks_position('none')
        plt.grid(True, which='minor', linestyle='-')
        plt.gcf().subplots_adjust(bottom=0.15)

        self.plot_confusion_matrix(labels, cm_normalized, title='Normalized confusion matrix')
        # show confusion matrix
        # plt.savefig('image/confusion_matrix.png', format='png')
        plt.show()


# 绘制混淆矩阵
def plotMatrix(attacks, y_test, y_pred):
    # attacks是整个数据集的标签集合，但是切分测试集的时候，某些标签数量很少，可能会被去掉，这里要剔除掉这些标签
    y_test_set = set(y_test)
    y_test_list = list(y_test_set)
    attacks_test = []
    for i in range(0, len(y_test_set)):
        attacks_test.append(attacks[y_test_list[i]])
    p = PlotConfusionMatrix()
    p.prepareWork(attacks_test, y_test, y_pred)


# 绘制混淆矩阵图形，attacks是标签列表，y_test是测试结果，y_pred是预测结果
