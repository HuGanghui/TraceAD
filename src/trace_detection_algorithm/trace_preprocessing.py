#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   trace_preprocessing.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/15 7:10 PM   hgh      1.0         None
import pickle

from utils import get_data_deviation, get_low_limit, get_upper_limit


class TraceDataPreProcessing:

    def __init__(self, data_path, sigma=3):
        self.data_path = data_path
        self.sigma = sigma
        self.id2mean = dict()
        self.id2vals = dict()
        self.id2baseline = dict()

    def get_id_mean(self):
        with open(self.data_path, "r") as f:
            i = 0
            for line in f:
                if i > 0:
                    lines = line.split(",")
                    cmdb_id = lines[1]
                    duration = int(lines[5])
                    if cmdb_id in self.id2mean:
                        temp_sum = self.id2mean[cmdb_id][0] * self.id2mean[cmdb_id][1] + duration
                        self.id2mean[cmdb_id][1] += 1
                        self.id2mean[cmdb_id][0] = temp_sum / self.id2mean[cmdb_id][1]
                    else:
                        self.id2mean[cmdb_id] = [duration, 1]
                else:
                    i += 1

    def get_id_vals(self):
        with open(self.data_path, "r") as f:
            i = 0
            for line in f:
                if i > 0:
                    lines = line.split(",")
                    cmdb_id = lines[1]
                    duration = int(lines[5])
                    if cmdb_id in self.id2vals:
                        self.id2vals[cmdb_id].append(duration)
                    else:
                        self.id2vals[cmdb_id] = [duration]
                else:
                    i += 1

    def get_id_upper_and_lower(self):
        self.get_id_mean()
        self.get_id_vals()
        for id in self.id2vals.keys():
            deviation = get_data_deviation(self.id2vals[id],
                                           self.id2mean[id][0],
                                           self.id2mean[id][1])

            self.id2baseline[id] = [get_low_limit(self.sigma, self.id2mean[id][0], deviation),
                                    get_upper_limit(self.sigma, self.id2mean[id][0], deviation)]
            print(id + str(self.id2baseline[id]))
        # self.id2baseline["gjjcore8"] = [-33.17473538445567, 71.36819537983898]
        # print("gjjcore8" + str(self.id2baseline["gjjcore8"]))
        self.dumps()

    def dumps(self):
        baseline_path = "./" + self.data_path.split("/")[-3] + "_" \
                        + self.data_path.split("/")[-1] + "_trace_baseline.txt"
        with open(baseline_path, "wb") as f:
            pickle.dump(self.id2baseline, f, 1)

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
    data_path = "../../data/system-a/trace/trace-0301.csv"
    test = TraceDataPreProcessing(data_path, 3)
    # test.vals_visiualization()
    test.get_id_upper_and_lower()
    print()