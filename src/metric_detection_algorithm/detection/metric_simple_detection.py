import os
import pickle
import time

from utils import find_timestamp_key


class MetricSimpleDetection:
    def __init__(self, ori_system, diff=True):
        """
        :param  ori_system: str     | a or b
        :param  diff      : boolean | True or False 是否使用一阶差分
        """
        curPath = os.path.abspath(os.path.dirname(__file__))
        rootPath = os.path.split(curPath)[0]
        self.diff = diff
        # sys.path.append(rootPath)
        if self.diff:
            model_save_path = rootPath + '/save_model/sigma_{}_diff.pkl'.format(ori_system)
        else:
            model_save_path = rootPath + '/save_model/sigma_{}.pkl'.format(ori_system)
        print("start load model: ")
        print(time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
        with open(model_save_path, 'rb') as f:
            self.clf = pickle.load(f)
        print("end load model")
        print(time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))

    def detect(self, seq, key):
        res = [0] * len(seq)
        for i in range(len(seq)):
            if key not in self.clf.keys():
                break
            if abs(seq[i] - self.clf[key]['avg']) > 3 * self.clf[key]['std']:
                res[i] = 1
        return res

    def metric_test(self, seq, cmdb_id, kpi_names):
        """
        :param seq:         list|多变量时序，窗口长度为5，长度为kpi数量
        :param cmdb_id      str | 异常网元名称
        :param kpi_names:   list|kpi的名的集合，长度同seq
        :return: kpi根因指标集合
        """
        y = []
        for i in range(len(seq)):

            key = '_'.join([cmdb_id, kpi_names[i]])
            if self.diff:
                tmp = [0]
                for j in range(0, len(seq[i]) - 1):
                    tmp.append(seq[i][j + 1] - seq[i][j])
            else:
                tmp = seq[i]
            y.append(self.detect(seq=tmp, key=key))

        res = []
        for i in range(len(y)):
            if 1 in y[i]:
                res.append(kpi_names[i])
        return res


def test_save_metric_a(test_metric_path, ne2ts2metrics):
    with open(test_metric_path, "r") as f:
        header = True
        i = 0
        for line in f:
            if not header:
                lines = line.split(",")
                msg = dict()
                msg["timestamp"] = int(lines[0])
                msg["cmdb_id"] = lines[1]
                msg["kpi_name"] = lines[2]
                msg["value"] = float(lines[3])

                ne = msg["cmdb_id"]
                ts = find_timestamp_key(msg["timestamp"])
                old_ts = ts - 300 * 3
                kpi_name = msg["kpi_name"]
                value = msg["value"]
                if ne not in ne2ts2metrics:
                    ne2ts2metrics[ne] = dict()
                if ts not in ne2ts2metrics[ne]:
                    ne2ts2metrics[ne][ts] = dict()
                if kpi_name not in ne2ts2metrics[ne][ts]:
                    ne2ts2metrics[ne][ts][kpi_name] = []
                # 进行淘汰工作
                # if old_ts in self.ne2ts2metrics[ne]:
                #     del self.ne2ts2metrics[ne][old_ts]
                ne2ts2metrics[ne][ts][kpi_name].append(value)
                i += 1
                if i % 100000 == 0:
                    print(i)
            else:
                header = False
    print()


if __name__ == '__main__':
    # seq = [
    #     [0.0, 1.0, 0.2, 0.5, 0.7],
    #     [0.0, 1.0, 0.2, 0.5, 0.7],
    #     [0.0, 1.0, 0.2, 0.5, 0.7],
    #     [0.0, 1.0, 0.2, 0.5, 0.7],
    #     [0.0, 1.0, 0.2, 0.5, 0.7],
    #     [0.0, 1.0, 0.2, 0.5, 0.7],
    #     [0.0, 1.0, 0.2, 0.5, 0.7],
    # ]
    # kpi_names = ['system.cpu.i_dle', 'system.load.5', 'system.load.1.pct', 'system.load.1', 'system.cpu.pct_usage',
    #              'system.load.norm.1', 'system.cpu.user']
    # cmdb_id = 'gjjcore9'
    # ori_system = 'a'
    # diff = True
    # test = MetricSimpleDetection("a", diff)
    # res_kpi_name = test.metric_test(seq, cmdb_id, kpi_names)
    # print(res_kpi_name)

    ad_ts_list = [1614290340, 1614295140, 1614307623, 1614316140, 1614329460, 1614353220]
    real_label_list = ["gjjcore2", "gjjha2", "gjjcore8", "gjjcore8", "gjjcore8", "gjjcore9"]
    test_metric_path = "../../../data/system-a/metric/metric_0226.csv"
    ne2ts2metrics = dict()
    test_save_metric_a(test_metric_path, ne2ts2metrics)
    test_ne = "gjjweb001"
    test_ts = 1614268800
    for ele in ne2ts2metrics[test_ne][test_ts]:
        print(ele)
    test_ne = "gjjweb001"
    test_ts = 1614268801
    if test_ne in ne2ts2metrics and test_ts in ne2ts2metrics:
        for ele in ne2ts2metrics[test_ne][test_ts]:
            print(ele)









