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
        result = self.stream_kpi["rr"] < 100
        if result:
            pass
            strtime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(self.stream_kpi["timestamp"]))
            print(self.stream_kpi["tc"] + " rr " + strtime)
        return result

    def sr_smaller_100(self):
        result = self.stream_kpi["sr"] < 100
        if result:
            pass
            strtime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(self.stream_kpi["timestamp"]))
            print(self.stream_kpi["tc"] + " sr " + strtime)
        return result

    def mrt_detect(self):
        result = self.stream_kpi["mrt"] > self.id2baseline[self.stream_kpi["tc"]][1] \
                 or self.stream_kpi["mrt"] < self.id2baseline[self.stream_kpi["tc"]][0]
        if result:
            pass
            strtime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(self.stream_kpi["timestamp"]))
            print(self.stream_kpi["tc"] + " mrt " + strtime)
        return result


def gold_test(test_data_path, baseline_path, system):
    i = 0
    simplemodel = SimpleThreadHoldDetection(baseline_path)
    with open(test_data_path, "r") as f:
        count = 0
        prev_time = None
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
                    strtime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(result))
                    if strtime != prev_time:
                        # print(strtime)
                        count += 1
                    prev_time = strtime
            else:
                i += 1
        print(count)


if __name__ == '__main__':
    system = "b"
    if system == "a":
        gold_test("../../data/system-a/kpi/kpi_0226.csv",
                  "./system-a_kpi_0301.csv_golddata_baseline.txt",
                  system)
    else:
        gold_test("../../data/system-b/kpi/kpi_0304.csv",
                  "./system-b_kpi_0129.csv_golddata_baseline.txt",
                  system)
