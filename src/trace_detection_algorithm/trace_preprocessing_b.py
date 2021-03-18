#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   trace_preprocessing_b.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/17 2:50 PM   hgh      1.0         None
import pickle
from queue import Queue

from trace_detection_algorithm.node import TraceNode
from utils import get_data_deviation, get_low_limit, get_upper_limit


class TraceDataPreProcessingB:

    def __init__(self, data_path, sigma=3):
        self.data_path = data_path
        self.sigma = sigma
        self.id2mean = dict()
        self.id2vals = dict()
        self.id2baseline = dict()
        self.id2baseline_result = dict()
        self.roots = dict()

    def pre_process(self):
        trace_id_dict = dict()
        i = 0
        with open(self.data_path, "r") as f:
            header = True
            for line in f:
                if not header:
                    lines = line.split(",")
                    msg = dict()
                    # msg["timestamp"] = int(lines[0])
                    msg["cmdb_id"] = lines[1]
                    if lines[1].startswith("My"):
                        print(str(i) + ": " + line)
                    msg["parent_id"] = lines[2]
                    msg["span_id"] = lines[3]
                    trace_id = lines[4]
                    msg["duration"] = float(lines[5])
                    if trace_id not in trace_id_dict:
                        trace_id_dict[trace_id] = []
                    trace_id_dict[trace_id].append(msg)
                    i += 1
                    if (i % 500000) == 0 and i != 0:
                        print(str(i) + "===========")
                        for ele in trace_id_dict:
                            parent_id2index = dict()
                            trace_datas = trace_id_dict[ele]
                            for index in range(0, len(trace_datas)):
                                parent_id2index[trace_datas[index]["span_id"]] = index
                            self.roots[ele] = dict()
                            self.create_tree(ele, trace_datas, parent_id2index)
                        self.get_id_upper_and_lower()
                        trace_id_dict.clear()
                    if (i % 30000000) == 0 and i != 0:
                        self.get_id_baseline_result(i)

                else:
                    header = False

        # if len(trace_id_dict) != 0:
        #     for ele in trace_id_dict:
        #         parent_id2index = dict()
        #         trace_datas = trace_id_dict[ele]
        #         for index in range(0, len(trace_datas)):
        #             parent_id2index[trace_datas[index]["span_id"]] = index
        #         self.roots[ele] = dict()
        #         self.create_tree(ele, trace_datas, parent_id2index)
        #     self.get_id_upper_and_lower()
        #     trace_id_dict.clear()

        self.get_id_baseline_result(i)

    def get_id_baseline_result(self, index):
        for id in self.id2baseline:
            lower_mean = 0
            upper_mean = 0
            size = 0
            for ele in self.id2baseline[id]:
                lower_mean += ele[0]
                upper_mean += ele[1]
                size += 1
            self.id2baseline_result[id] = [lower_mean / size, upper_mean / size]

            print(id + str(self.id2baseline_result[id]))
        self.dumps(index)

    def dumps(self, index):
        baseline_path_suffix = "./" + self.data_path.split("/")[-3] + "_" \
                        + self.data_path.split("/")[-1] + "_" + str(index)
        baseline_path = baseline_path_suffix + "_trace_baseline.txt"
        with open(baseline_path, "wb") as f:
            pickle.dump(self.id2baseline_result, f, 1)

        self._log(baseline_path_suffix)

    def _log(self, baseline_path_suffix):
        baseline_path = baseline_path_suffix + "_trace_baseline_log.txt"
        with open(baseline_path, "w") as f:
            for id in self.id2baseline_result:
                f.write(id + ": " + str(self.id2baseline_result[id]) + "\n")

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
        self.roots.clear()

    def _get_net_duration(self, node):
        q = Queue()
        q.put(node)
        while not q.empty():
            temp = q.get()
            sub_duration = 0
            for child in temp.childs:
                sub_duration += child.duration
                q.put(child)
            temp.duration -= sub_duration
            self.get_id_mean(temp)
            self.get_id_vals(temp)

    def get_id_mean(self, node):
        if node.cmdb_id in self.id2mean:
            temp_sum = self.id2mean[node.cmdb_id][0] * self.id2mean[node.cmdb_id][1] + node.duration
            self.id2mean[node.cmdb_id][1] += 1
            self.id2mean[node.cmdb_id][0] = temp_sum / self.id2mean[node.cmdb_id][1]
        else:
            self.id2mean[node.cmdb_id] = [node.duration, 1]

    def get_id_vals(self, node):
        if node.cmdb_id in self.id2vals:
            self.id2vals[node.cmdb_id].append(node.duration)
        else:
            self.id2vals[node.cmdb_id] = [node.duration]

    def get_id_upper_and_lower(self):
        for id in self.id2vals.keys():
            deviation = get_data_deviation(self.id2vals[id],
                                           self.id2mean[id][0],
                                           self.id2mean[id][1])
            if id not in self.id2baseline:
                self.id2baseline[id] = []
            self.id2baseline[id].append((get_low_limit(self.sigma, self.id2mean[id][0], deviation),
                                        get_upper_limit(self.sigma, self.id2mean[id][0], deviation)))
            # print(id + str(self.id2baseline[id]))
        self.id2mean.clear()
        self.id2vals.clear()

    def vals_visiualization(self):
        import plotly.graph_objects as go
        import plotly.io as pio
        for id in self.id2vals.keys():
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=[i for i in range(0, len(self.id2vals[id][0]))],
                                     y=self.id2vals[id][1], name="trace duration"))
            save_dir_path = "./visualization/"
            fig_html = save_dir_path + self.data_path.split("/")[-1] + "_" + id + ".html"
            pio.write_html(fig, file=fig_html)


if __name__ == '__main__':
    data_path = "../../data/system-b/trace/trace_0311.csv"
    test = TraceDataPreProcessingB(data_path, 3)
    # test.vals_visiualization()
    # test.get_id_upper_and_lower()
    test.pre_process()
    print()