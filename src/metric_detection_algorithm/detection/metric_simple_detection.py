import os
import pickle
import time

import numpy as np


class MetricSimpleDetection:
    def __init__(self, ori_system):
        """
        :param  ori_system: str |a or b
        """
        curPath = os.path.abspath(os.path.dirname(__file__))
        rootPath = os.path.split(curPath)[0]
        # sys.path.append(rootPath)
        model_save_path = rootPath + '/save_model/rf_{}.pkl'.format(ori_system)
        print("start load model: ")
        print(time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
        with open(model_save_path, 'rb') as f:
            self.clf = pickle.load(f)
        print("end load model")
        print(time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))

    # 根据均值、标准差,求指定范围的正态分布概率值
    @staticmethod
    def normfun(x, mu, sigma):
        if sigma == 0:
            sigma = 0.0001
        pdf = np.exp(-((x - mu) ** 2) / (2 * sigma ** 2)) / (sigma * np.sqrt(2 * np.pi))
        return pdf

    @staticmethod
    def feature(seq):
        x = []
        for i in seq:
            mu = np.average(i)
            sigma = np.std(i)
            gauss_feature = MetricSimpleDetection.normfun(i, mu, sigma).tolist()
            tmp = i
            tmp.extend(gauss_feature)
            tmp.append(mu)
            tmp.append(sigma)
            x.append(tmp)
        return x

    def metric_test(self, seq, kpi_names):
        """
        :param seq:         list|多变量时序，窗口长度为5，长度为kpi数量
        :param kpi_names:   list|kpi的名的集合，长度同seq
        :return: kpi根因指标集合
        """

        x = self.feature(seq)
        y = self.clf.predict(x)
        res = []
        for i in range(len(y)):
            if y[i] == 1:
                res.append(kpi_names[i])
        return res


if __name__ == '__main__':
    seq = [
        [0.0, 1.0, 0.2, 0.5, 0.7],
        [0.0, 1.0, 0.2, 0.5, 0.7],
        [0.0, 1.0, 0.2, 0.5, 0.7],
        [0.0, 1.0, 0.2, 0.5, 0.7],
        [0.0, 1.0, 0.2, 0.5, 0.7],
        [0.0, 1.0, 0.2, 0.5, 0.7],
        [0.0, 1.0, 0.2, 0.5, 0.7],
    ]
    kpi_names = ['system.cpu.i_dle', 'system.load.5', 'system.load.1.pct', 'system.load.1', 'system.cpu.pct_usage',
                 'system.load.norm.1', 'system.cpu.user']
    ori_system = 'a'

    test = MetricSimpleDetection("a")
    res_kpi_name = test.metric_test(seq, kpi_names)
    print(res_kpi_name)