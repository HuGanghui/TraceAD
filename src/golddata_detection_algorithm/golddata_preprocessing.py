#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   golddata_preprocessing.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/12 4:13 PM   hgh      1.0         None
import time

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots


class GoldDataPreProcessing:
    def __init__(self, data_path, fault_label_path):
        self.data_path = data_path
        self.fault_label_path = fault_label_path
        self.dataFrame = None
        self.DataFrameList = None

    def get_fault_time_list(self):
        falut_time_list = list()
        with open(self.fault_label_path) as f:
            for line in f:
                line = line.strip()
                falut_time_list.append(int(time.mktime(time.strptime(line, '%Y/%m/%d %H:%M:%S'))))
        return falut_time_list

    def get_fault_time_mrs_list(self, fault_time_list, dataFrame):
        fault_time_mrt_list = list()
        for fault_time in fault_time_list:
            fault_time_mrs = dataFrame[dataFrame["timestamp"] == fault_time]["mrt"].values
            if len(fault_time_mrs) == 1:
                fault_time_mrt_list.append(fault_time_mrs[0])
            else:
                fault_time_mrt_list.append(100)
        return fault_time_mrt_list

    def get_data_from_file(self):
        self.dataFrame = pd.read_csv(self.data_path, engine='python')
        self.get_dataframe_list()

    def get_dataframe_list(self):
        group = self.dataFrame.groupby("tc")
        self.DataFrameList = list()
        for ele in list(group):
            self.DataFrameList.append(ele[1])
        self._sort_values()
        self._change_ts_to_string()

    def _sort_values(self):
        """
        使其按时间戳从小到大排序

        """
        for ele in self.DataFrameList:
            ele = ele.sort_values("timestamp")
            ele = ele.reset_index(drop=True)

    def _change_ts_to_string(self):
        for ele in self.DataFrameList:
            ele["ts"] = ele["timestamp"].apply(lambda x: time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(x)))

    def rr_or_sr_smaller_100(self, dataFrame):
        sr_smaller_100 = dataFrame.loc[dataFrame["sr"] < 100, ["ts", "mrt", "sr"]]
        rr_smaller_100 = dataFrame.loc[dataFrame["rr"] < 100, ["ts", "mrt", "rr"]]
        print(len(sr_smaller_100), len(rr_smaller_100))
        return sr_smaller_100, rr_smaller_100

    def simple_display(self):
        fig = make_subplots(rows=3, cols=1,
                            shared_xaxes=True)
        i = 1
        for ele in self.DataFrameList:
            fig.add_trace(go.Scatter(x=ele["ts"],
                                     y=ele["mrt"], name="tc 响应时间: " + str(i)),
                          row=i, col=1)
            sr_smaller_100, rr_smaller_100 = self.rr_or_sr_smaller_100(ele)
            fig.add_trace(go.Scatter(x=sr_smaller_100["ts"],
                                     y=sr_smaller_100["mrt"], name="tc sr < 100 : " + str(i),
                                     mode='markers', marker={"symbol": "circle", "opacity": 0.5}),
                          row=i, col=1)
            fig.add_trace(go.Scatter(x=rr_smaller_100["ts"],
                                     y=rr_smaller_100["mrt"], name="tc rr < 100 : " + str(i),
                                     mode='markers', marker={"symbol": "x", "opacity": 0.5}),
                          row=i, col=1)

            fault_time_list = self.get_fault_time_list()
            fault_time_mrt_list = self.get_fault_time_mrs_list(fault_time_list, ele)
            fault_strtime_list = list()
            # 为了可视化对齐x轴
            for timestamp in fault_time_list:
                fault_strtime_list.append(time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(timestamp)))
            fig.add_trace(go.Scatter(x=fault_strtime_list,
                                     y=fault_time_mrt_list, name="fault time : " + str(i),
                                     mode='markers', marker={"symbol": "cross", "opacity": 0.5}),
                          row=i, col=1)
            i = i + 1
        save_dir_path = "./"
        fig_html = save_dir_path + self.data_path.split("/")[-1] + ".html"
        pio.write_html(fig, file=fig_html)


if __name__ == '__main__':
    data_path = "../../data/system-a/kpi/kpi_0226.csv"
    fault_time_path = "../../data/system-a/system-a-0226-fault-time.txt"
    test = GoldDataPreProcessing(data_path, fault_time_path)
    test.get_data_from_file()
    test.simple_display()