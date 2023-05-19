import matplotlib.pyplot as plt
import numpy as np
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import GridSearchCV

def tuning_hyper(X_train, y_train,X_test, y_test,maxn):
    test = []
    for i in range(maxn):
        clf = DecisionTreeClassifier(max_depth=i + 1

                                     , criterion="entropy"

                                     , random_state=30

                                     , splitter="random"

                                     )
        clf = clf.fit(X_train, y_train)
        score = clf.score(X_test, y_test)
        test.append(score)
    plt.plot(range(1, maxn + 1), test, color="red", label="max_depth")
    plt.legend()
    plt.show()


def martix_search(Xtrain, Ytrain):

    gini_thresholds = np.linspace(0, 0.5, 20)  # 基尼系数的边界
    # entropy_thresholds = np.linespace(0, 1, 50)

    # 一串参数和这些参数对应的，我们希望网格搜索来搜索的参数的取值范围
    parameters = {'splitter': ('best', 'random')
        , 'criterion': ("gini", "entropy")
        , "max_depth": [*range(1, 10)]
        , 'min_samples_leaf': [*range(1, 50, 5)]
        , 'min_impurity_decrease': [*gini_thresholds]}

    clf = DecisionTreeClassifier(random_state=25)  # 实例化决策树
    GS = GridSearchCV(clf, parameters, cv=10)  # 实例化网格搜索，cv指的是交叉验证
    GS.fit(Xtrain, Ytrain)
    print(GS.best_params_)  # 从我们输入的参数和参数取值的列表中，返回最佳组合
    print(GS.best_score_)  # 网格搜索后的模型的评判标准
