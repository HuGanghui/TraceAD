#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   trace_simple_detection_b.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/17 5:08 PM   hgh      1.0         None

import pickle
import time
from queue import Queue

from trace_detection_algorithm.node import TraceNode
from utils import find_timestamp_key


class TraceSimpleDetectionB:
    """
    根据对应网元的duration超过3-Sigma来判断网元是否异常
    """

    def __init__(self, baseline_path, ts2traces):
        self.ad_ele = dict()
        # TODO 考虑到线程安全问题，因此容器需要保证线程安全
        self.timestamp2traces = ts2traces
        self.roots = dict()
        if baseline_path is not None:
            with open(baseline_path, "rb") as f:
                self.id2baseline = pickle.load(f)

    # for test
    def save_trace_b(self, trace_data):
        ts = find_timestamp_key(trace_data["timestamp"])
        if ts not in self.timestamp2traces:
            self.timestamp2traces[ts] = dict()
        if trace_data["trace_id"] not in self.timestamp2traces[ts]:
            self.timestamp2traces[ts][trace_data["trace_id"]] = []
        # TODO 是不是无需字典
        self.timestamp2traces[ts][trace_data["trace_id"]].append({"cmdb_id": trace_data["cmdb_id"],
                                                                  "parent_id": trace_data["parent_id"],
                                                                  "span_id": trace_data["span_id"],
                                                                  "duration": trace_data["duration"]})

    def detect_b(self, ad_timestamp):
        ts = find_timestamp_key(ad_timestamp)
        curr_ts = ts
        if curr_ts in list(self.timestamp2traces.keys()):
            for ele in list(self.timestamp2traces[curr_ts].keys()):
                parent_id2index = dict()
                trace_datas = self.timestamp2traces[curr_ts][ele]
                for i in range(0, len(trace_datas)):
                    parent_id2index[trace_datas[i]["span_id"]] = i
                self.roots[ele] = dict()
                self.create_tree(ele, trace_datas, parent_id2index)
        self.duration_detect_b()
        # TODO 不不仅仅考虑异常程度，还考虑异常数量
        result_ = sorted(self.ad_ele.items(), key=lambda kv: (kv[1][0], kv[0]), reverse=True)
        self.ad_ele.clear()
        result = []
        for ele in result_[:4]:
            # print(str(ele[0]) + ": " + str(ele[1]))
            result.append(ele[0])
        return result

    def duration_detect_b(self):
        for ele in self.roots:
            for root_node in self.roots[ele]:
                node = self.roots[ele][root_node]
                self._duration_detect_b(node)

    def _duration_detect_b(self, node):
        result = False
        if node.cmdb_id in self.id2baseline:
            result = node.duration > self.id2baseline[node.cmdb_id][1]
        if result:
            # print(str(node.cmdb_id) + ": " + str(node.duration))
            degree = abs(node.duration - self.id2baseline[node.cmdb_id][1]) / self.id2baseline[node.cmdb_id][1]
            if node.cmdb_id not in self.ad_ele:
                self.ad_ele[node.cmdb_id] = [degree, 0]
            elif degree > self.ad_ele[node.cmdb_id][0]:
                self.ad_ele[node.cmdb_id][0] = degree
            self.ad_ele[node.cmdb_id][1] = self.ad_ele[node.cmdb_id][1] + 1

        if len(node.childs) != 0:
            for child in node.childs:
                self._duration_detect_b(child)

    def create_tree(self, trace_id, trace_datas, parent_id2index):
        node_map = dict()
        for index in range(0, len(trace_datas)):
            self.create_node(trace_id, trace_datas, index, parent_id2index, node_map)

        self.get_net_duration(trace_id)

    def create_node(self, trace_id, trace_datas, index, parent_id2index, node_map):
        span_id = trace_datas[index]["span_id"]
        if span_id in node_map:
            return
        node_map[span_id] = TraceNode(span_id,
                                      trace_datas[index]["cmdb_id"],
                                      trace_datas[index]["duration"])
        span_node = node_map[span_id]
        # TODO 可能后面再加上父指针
        parent_id = trace_datas[index]["parent_id"]
        if parent_id == span_id:
            self.roots[trace_id][span_id] = span_node
            return

        if parent_id not in parent_id2index:
            self.roots[trace_id][span_id] = span_node
            return

        if parent_id not in node_map:
            self.create_node(trace_id, trace_datas, parent_id2index[parent_id],
                             parent_id2index, node_map)

        p = node_map[parent_id]
        p.childs.append(span_node)

    def get_net_duration(self, trace_id):
        nodes = self.roots[trace_id]
        for node in nodes:
            self._get_net_duration(nodes[node])

    @staticmethod
    def _get_net_duration(node):
        q = Queue()
        q.put(node)
        while not q.empty():
            temp = q.get()
            sub_duration = 0
            for child in temp.childs:
                sub_duration += child.duration
                q.put(child)
            temp.duration -= sub_duration


def trace_test_b(test_data_path, baseline_path, id2trace):
    print("start get trace data")
    trace_simple_model = TraceSimpleDetectionB(baseline_path, id2trace)
    with open(test_data_path, "r") as f:
        header = True
        i = 0
        for line in f:
            if not header:
                lines = line.split(",")
                msg = dict()
                msg["timestamp"] = int(lines[0])
                msg["cmdb_id"] = lines[1]
                if lines[1].startswith("Mysql"):
                    print(str(i) + line)
                msg["parent_id"] = lines[2]
                msg["span_id"] = lines[3]
                msg["trace_id"] = lines[4]
                msg["duration"] = float(lines[5])
                trace_simple_model.save_trace_b(msg)
                i += 1
                if i % 10000 == 0:
                    print(i)
            else:
                header = False
    return trace_simple_model


if __name__ == '__main__':
    trace_test_data_path = "../../data/system-b/trace/trace_0304.csv"
    # trace_baseline_path = "./system-b_trace_0311.csv_22266996_trace_baseline.txt"
    trace_baseline_path = "./system-b_trace_0129.csv_60000000_trace_baseline.txt"
    trace_model = trace_test_b(trace_test_data_path, trace_baseline_path, dict())
    ad_ts_list = [1614797100, 1614798240, 1614815220, 1614818340, 1614823500, 1614858540, 1614856920]
    # ad_ts_list = [1614797100]
    real_label_list = ["Mysql02-内存使用过高", "Tomcat02-JVM OOM", "Tomcat01-JVM OOM", "MG02-CPU攀升", "Mysql01-网络隔离", "Tomcat02-网络延迟", "Tomcat03-磁盘IO过高"]

    i = 0
    print(trace_baseline_path)
    for ad_ts in ad_ts_list:
        print(ad_ts)
        strtime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(ad_ts))
        print(strtime)
        print("detected:")
        print(trace_model.detect_b(ad_ts))
        print("real: \n" + real_label_list[i])
        i += 1
        print("=================================")
