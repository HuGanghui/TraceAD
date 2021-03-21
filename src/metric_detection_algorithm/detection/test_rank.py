import os
import pickle
import time

import numpy as np
import metric_detection_algorithm.detection.sranodec as anom


class MetricSimpleDetection:
    def __init__(self, ori_system, method='shesd', diff=True):
        """
        :param  ori_system: str     | a or b
        :param  method    : str     | 'sigma' 'shesd' 'sr'
        :param  diff      : boolean | True or false 是否使用一阶差分
        """
        curPath = os.path.abspath(os.path.dirname(__file__))
        rootPath = os.path.split(curPath)[0]
        self.method = method
        if self.method == 'sigma' or self.method == 'shesd':
            self.diff = diff
            # sys.path.append(rootPath)

            model_save_path = rootPath + '/save_model/sigma_shesd_{}.pkl'.format(ori_system)
            print("start load model: ")
            print(time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
            with open(model_save_path, 'rb') as f:
                self.clf = pickle.load(f)
            print("end load model")
            print(time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
        if self.method == 'sr':
            self.diff = False

    @staticmethod
    def sr_detect(time_series, summary_way='max', amp_window=20, series_window=20, score_window=40, threriod=0.01):
        """
        :param time_series: np.array
        :param summary_way 'max' or 'sum'
        :param amp_window: less than period
        :param series_window: (maybe) as same as period
        :param score_window: a number enough larger than period
        :param threriod: 调节异常阈值，越小表示异常越少，越大表示异常越多
        :return: bool.True:Normal; False:Abnormal
        """
        test_signal = np.array(time_series)
        tmp = []
        for i in range(test_signal.shape[0]):
            tmp.append(test_signal - test_signal[0])
        if np.count_nonzero(np.array(tmp)) == 0:
            return 0
        # less than period
        amp_window_size = amp_window
        series_window_size = series_window
        score_window_size = score_window
        spec = anom.Silency(amp_window_size, series_window_size, score_window_size)
        score = spec.generate_anomaly_score(test_signal)
        window_summary = 0.0

        if summary_way == 'max':
            window_summary = max(score)
        elif summary_way == 'sum':
            window_summary = sum(abs(x) for x in score)

        return window_summary
        # index_changes = np.where(score > np.percentile(score, 100*(1-threriod)))[0]

        # if (test_signal.shape[0] - 1) in index_changes:
        #     """
        #     Abnormal
        #     """
        #     return False, window_summary
        # else:
        #     """
        #     Normal
        #     """
        #     return True, window_summary    

    def detect(self, seq, key, sigma=10):
        flag = 0
        if self.method == 'sigma':
            res = [0] * len(seq)
            if self.diff:
                for i in range(len(seq)):
                    if key not in self.clf.keys():
                        break
                    if abs(seq[i] - self.clf[key]['avg']) > sigma * self.clf[key]['std']:
                        flag = 1
                        if self.clf[key]['std'] == 0:
                            res[i] = abs(seq[i] - self.clf[key]['avg'])
                            continue
                        res[i] = abs(seq[i] - self.clf[key]['avg']) / self.clf[key]['std']
            else:
                for i in range(len(seq)):
                    if key not in self.clf.keys():
                        break
                    # print(self.clf[key])
                    if abs(seq[i] - self.clf[key]['avg_diff']) > sigma * self.clf[key]['std_diff']:
                        flag = 1
                        if self.clf[key]['std_diff'] == 0:
                            res[i] = abs(seq[i] - self.clf[key]['avg_diff'])
                            continue
                        res[i] = abs(seq[i] - self.clf[key]['avg_diff']) / self.clf[key]['std_diff']
            if flag:
                return max(res)
            else:
                return 0

        if self.method == 'shesd':
            res = [0] * len(seq)
            for i in range(len(seq)):
                if key not in self.clf.keys():
                    break
                if abs(seq[i] - self.clf[key]['mid']) > sigma * self.clf[key]['mad']:
                    flag = 1
                    if self.clf[key]['mad'] == 0:
                        res[i] = abs(seq[i] - self.clf[key]['mid'])
                        continue
                    res[i] = abs(seq[i] - self.clf[key]['mid']) / self.clf[key]['mad']
            if flag:
                return max(res)
            else:
                return 0

        if self.method == 'sr':
            return self.sr_detect(seq)

    def metric_test(self, seq, cmdb_id, kpi_names, sigma=10, percent=0.3):
        """
        :param seq:         list |多变量时序，窗口长度为5，长度为kpi数量
        :param cmdb_id      str  | 异常网元名称
        :param kpi_names:   list |kpi的名的集合，长度同seq
        :param percent:     float|输出集合的阈值
        :return: kpi根因指标集合
        """
        y = []

        for i in range(len(seq)):
            key = '_'.join([cmdb_id, kpi_names[i]])
            if len(seq[i]) == 0:
                y.append(0)
                continue
            if self.diff:
                tmp = [0]
                for j in range(0, len(seq[i]) - 1):
                    tmp.append(seq[i][j + 1] - seq[i][j])
            else:
                tmp = seq[i]
            y.append(self.detect(seq=tmp, key=key, sigma=sigma))

        y, kpi_names = (list(t) for t in zip(*sorted(zip(y, kpi_names))))
        idx = 0
        flag = 0
        for i in range(len(y)):
            if y[i] != 0:
                flag = 1
                idx = i
                break
        if not flag:
            res = []
            return res
        tmp = kpi_names[idx:]

        res = tmp[int(len(tmp) * (1 - percent)):]
        return res[::-1]


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
    seq = [
        [], [], [], [], [], [], []
    ]
    kpi_names = ['system.cpu.i_dle', 'system.load.5', 'system.load.1.pct', 'system.load.1', 'system.cpu.pct_usage',
                 'system.load.norm.1', 'system.cpu.user']
    cmdb_id = 'gjjcore9'
    ori_system = 'a'
    diff = True
    method = 'sigma'
    test = MetricSimpleDetection("a", method, diff)
    res_kpi_name = test.metric_test(seq, cmdb_id, kpi_names)
    print(res_kpi_name)
