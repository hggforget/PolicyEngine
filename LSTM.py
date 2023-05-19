import torch
import torch.nn as nn
import torch.utils.data as Data


def get_train_data():
    """得到训练数据，这里使用随机数生成训练数据，由此导致最终结果并不好"""

    def get_tensor_from_pd(dataframe_series) -> torch.Tensor:
        return torch.tensor(data=dataframe_series.values)

    import numpy as np
    import pandas as pd
    from sklearn import preprocessing
    # 生成训练数据x并做标准化后，构造成dataframe格式，再转换为tensor格式
    df = pd.DataFrame(data=preprocessing.StandardScaler().fit_transform(np.random.randint(0, 10, size=(200, 5))))
    y = pd.Series(np.random.randint(0, 2, 200))
    return get_tensor_from_pd(df).float(), get_tensor_from_pd(y).float()


class LSTM(nn.Module):
    def __init__(self, input_size=5, hidden_layer_size=100, output_size=1):
        """
        LSTM二分类任务
        :param input_size: 输入数据的维度
        :param hidden_layer_size:隐层的数目
        :param output_size: 输出的个数
        """
        super().__init__()
        self.hidden_layer_size = hidden_layer_size
        self.lstm = nn.LSTM(input_size, hidden_layer_size)
        self.linear = nn.Linear(hidden_layer_size, output_size)
        self.sigmoid = nn.Sigmoid()

    def forward(self, input_x):
        input_x = input_x.view(len(input_x), 1, -1)
        hidden_cell = (torch.zeros(1, 1, self.hidden_layer_size),  # shape: (n_layers, batch, hidden_size)
                       torch.zeros(1, 1, self.hidden_layer_size))
        lstm_out, (h_n, h_c) = self.lstm(input_x, hidden_cell)
        linear_out = self.linear(lstm_out.view(len(input_x), -1))  # =self.linear(lstm_out[:, -1, :])
        predictions = self.sigmoid(linear_out)
        return predictions


if __name__ == '__main__':
    # 得到数据
    x, y = get_train_data()
    train_loader = Data.DataLoader(
        dataset=Data.TensorDataset(x, y),  # 封装进Data.TensorDataset()类的数据，可以为任意维度
        batch_size=20,  # 每块的大小
        shuffle=True,  # 要不要打乱数据 (打乱比较好)
        num_workers=2,  # 多进程（multiprocess）来读数据
    )
    # 建模三件套：loss，优化，epochs
    model = LSTM()  # 模型
    loss_function = nn.BCELoss()  # loss
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)  # 优化器
    epochs = 20
    # 开始训练
    model.train()
    for i in range(epochs):
        for seq, labels in train_loader:
            optimizer.zero_grad()
            y_pred = model(seq).squeeze()  # 压缩维度：得到输出，并将维度为1的去除
            single_loss = loss_function(y_pred, labels)
            # 若想要获得类别，二分类问题使用四舍五入的方法即可：print(torch.round(y_pred))
            single_loss.backward()
            optimizer.step()
            print("Train Step:", i, " loss: ", single_loss)
    # 开始验证
    model.eval()
    for i in range(epochs):
        for seq, labels in train_loader:  # 这里偷个懒，就用训练数据验证哈！
            y_pred = model(seq).squeeze()  # 压缩维度：得到输出，并将维度为1的去除
            single_loss = loss_function(y_pred, labels)
            print("EVAL Step:", i, " loss: ", single_loss)
            print(torch.round(y_pred)," label: ",labels)
