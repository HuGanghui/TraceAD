#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   trace_simple_detection.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/16 11:53 AM   hgh      1.0         None
import pickle
import time
from utils import find_timestamp_key


class TraceSimpleDetection:
    """
    根据对应网元的duration超过3-Sigma来判断网元是否异常
    """
    def __init__(self, baseline_path, ts2traces):
        self.ad_ele = dict()
        # TODO 考虑到线程安全问题，因此容器需要保证线程安全
        self.timestamp2traces = ts2traces
        if baseline_path is not None:
            with open(baseline_path, "rb") as f:
                self.id2baseline = pickle.load(f)

    # for test
    def save_trace_a(self, trace_data):
        """
        针对a系统的traces都是只有一条，因此直接保存该网元以及对应的duration，后续用来判断是否异常
        :param trace_data: 单条trace数据
        :return:
        """
        ts = find_timestamp_key(trace_data["timestamp"])
        if ts not in self.timestamp2traces:
            self.timestamp2traces[ts] = []
        self.timestamp2traces[ts].append((trace_data["cmdb_id"], trace_data["duration"]))

    def detect(self, ad_timestamp):
        ts = find_timestamp_key(ad_timestamp)
        # 目前只考虑当前区间
        prev_ts = ts - 300
        curr_ts = ts
        next_ts = ts + 300
        # if prev_ts in self.timestamp2traces:
        #     for ele in self.timestamp2traces[prev_ts]:
        #         self.duration_detect(ele[0], ele[1])
        if curr_ts in self.timestamp2traces:
            for ele in self.timestamp2traces[curr_ts]:
                self.duration_detect(ele[0], ele[1])
        # if next_ts in self.timestamp2traces:
        #     for ele in self.timestamp2traces[next_ts]:
        #         self.duration_detect(ele[0], ele[1])
        result_ = sorted(self.ad_ele.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)
        self.ad_ele.clear()
        result = []
        for ele in result_[:4]:
            result.append(ele[0])
        return result

    def duration_detect(self, ele_id, ele_duration):
        result = False
        if ele_id in self.id2baseline:
            # 一般情况应该是高于上界，目前只使用上界
            result = ele_duration > self.id2baseline[ele_id][1]
        if result:
            degree = abs(result - self.id2baseline[ele_id][1]) / self.id2baseline[ele_id][1]
            if ele_id not in self.ad_ele or degree > self.ad_ele[ele_id]:
                self.ad_ele[ele_id] = degree


def trace_test_a(test_data_path, baseline_path, id2traces):
    print("start get trace data")
    trace_simple_model = TraceSimpleDetection(baseline_path, id2traces)
    with open(test_data_path, "r") as f:
        header = True
        for line in f:
            if not header:
                lines = line.split(",")
                msg = dict()
                msg["cmdb_id"] = lines[1]
                msg["timestamp"] = int(lines[0])
                msg["duration"] = float(lines[5])
                trace_simple_model.save_trace_a(msg)
            else:
                header = False
    return trace_simple_model


if __name__ == '__main__':
    trace_test_data_path = "../../data/system-a/trace/trace-0226.csv"
    trace_baseline_path = "./system-a_trace-0227.csv_trace_baseline.txt"
    trace_model = trace_test_a(trace_test_data_path, trace_baseline_path, dict())
    ad_ts_list = [1614290340, 1614295140, 1614307623, 1614316140, 1614329460, 1614353220]
    real_label_list = ["gjjcore2", "gjjha2 (根本不存在这个网元的trace，先不管)", "gjjcore8", "gjjcore8", "gjjcore8", "gjjcore9"]
    i = 0
    for ad_ts in ad_ts_list:
        print(ad_ts)
        strtime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(ad_ts))
        print(strtime)
        print("detected:")
        print(trace_model.detect(ad_ts))
        print("real: \n" + real_label_list[i])
        i += 1
        print("=================================")

