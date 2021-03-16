#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   gold_trace_test.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/16 1:01 PM   hgh      1.0         None
import time

from golddata_detection_algorithm import SimpleThreadHoldDetection
from trace_detection_algorithm.trace_simple_detection import trace_test
system = "a"
gold_test_data_path = "../data/system-a/kpi/kpi_0226.csv"
gold_baseline_path = "./golddata_detection_algorithm/system-a_kpi_0301.csv_golddata_baseline.txt"
trace_test_data_path = "../data/system-a/trace/trace-0226.csv"
trace_baseline_path = "./trace_detection_algorithm/system-a_trace-0227.csv_trace_baseline.txt"


def gold_test(test_data_path, baseline_path, system, trace_model):
    i = 0
    simplemodel = SimpleThreadHoldDetection(baseline_path)
    with open(test_data_path, "r") as f:
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
                gold_ad_ts = simplemodel.detect(msg)
                if gold_ad_ts is not None:
                    print(gold_ad_ts)
                    strtime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(gold_ad_ts))
                    print(strtime)
                    print(trace_model.detect(gold_ad_ts))
            else:
                i += 1


if __name__ == '__main__':
    trace_model = trace_test(trace_test_data_path, trace_baseline_path, system)
    gold_test(gold_test_data_path, gold_baseline_path, system, trace_model)
