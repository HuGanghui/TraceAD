#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   simple_threadhold_detection.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/13 12:07 PM   hgh      1.0         None
import pickle
import time


class SimpleThreadHoldDetection:
    """
    根据sr、rr小于100或者响应时间超过3-Sigma来判断黄金指标是否异常
    """
    def __init__(self, baseline_path):
        self.stream_kpi = None
        with open(baseline_path, "rb") as f:
            self.id2baseline = pickle.load(f)

    def detect(self, stream_kpi):
        self.stream_kpi = stream_kpi
        if self.rr_smaller_100() or self.sr_smaller_100() \
                or self.mrt_detect():
        # if self.mrt_detect():
            return self.stream_kpi["timestamp"]
        else:
            return None

    def rr_smaller_100(self):
        return self.stream_kpi["rr"] < 100

    def sr_smaller_100(self):
        return self.stream_kpi["sr"] < 100

    def mrt_detect(self):
        result = self.stream_kpi["mrt"] > self.id2baseline[self.stream_kpi["tc"]][1] \
                 or self.stream_kpi["mrt"] < self.id2baseline[self.stream_kpi["tc"]][0]
        if result:
            pass
            # print(self.stream_kpi["tc"])
        return result


def gold_test(test_data_path, baseline_path, label_ts_list, system, avoid_over_alert=180):
    i = 0
    simplemodel = SimpleThreadHoldDetection(baseline_path)
    with open(test_data_path, "r") as f:
        count = 0
        detect_count = 0
        prev_ts = None
        for line in f:
            if i > 0:
                lines = line.split(",")
                msg = dict()
                msg["timestamp"] = int(lines[0])
                msg["rr"] = float(lines[1])
                msg["sr"] = float(lines[2])
                if system == "a":
                    msg["count"] = float(lines[3])
                else:
                    msg["cnt"] = float(lines[3])
                msg["mrt"] = float(lines[4])
                msg["tc"] = lines[5].strip()
                result = simplemodel.detect(msg)
                if result is not None:
                    if prev_ts is None or result > (prev_ts + avoid_over_alert):
                        strtime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(result))
                        for lable_ts in label_ts_list:
                            if 0 <= result - lable_ts <= 600:
                                print(strtime)
                                detect_count += 1
                                break
                        count += 1
                        prev_ts = result
            else:
                i += 1
        print(detect_count)
        print(count)


if __name__ == '__main__':
    system = "a"
    if system == "a":
        label_ts_list = [1614287762, 1614290280, 1614295140, 1614307623, 1614316140, 1614329460, 1614353220]
        gold_test("../../data/system-a/kpi/kpi_0226.csv",
                  "./system-a_kpi_0301.csv_golddata_baseline.txt",
                  label_ts_list,
                  "a")
    else:
        label_ts_list = [1614797100, 1614798240, 1614815220, 1614818340, 1614823500, 1614856920, 1614858540]
        gold_test("../../data/system-b/kpi/kpi_0304.csv",
                  "./system-b_kpi_0129.csv_golddata_baseline.txt",
                  label_ts_list,
                  "b", 180)
