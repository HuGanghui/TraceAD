#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   golddata_preprocessing.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/15 9:08 PM   hgh      1.0         None
import pickle

import pandas as pd

from utils import get_low_limit, get_upper_limit


class GOLDDataPreProcessing:
    """
    获取黄金指标不同tc的均值和上下基线
    """
    def __init__(self, data_path, sigma=3):
        """
        :param data_path: 文件路径，代表用哪一天的数据来获取该系统的均值和上下界限
        :param sigma:
        """
        self.data_path = data_path
        self.sigma = sigma
        self.id2mean = dict()
        self.id2std = dict()
        self.id2baseline = dict()

    def get_data_from_file(self):
        self.dataFrame = pd.read_csv(self.data_path, engine='python')
        self.get_dataframe_list()

    def get_dataframe_list(self):
        group = self.dataFrame.groupby("tc")
        keys = self.dataFrame["tc"].unique()
        dataframes = list(group)
        for ele in dataframes:
            tc = ele[1]["tc"].unique()[0]
            print(tc)
            self.id2mean[tc] = ele[1]["mrt"].mean()
            self.id2std[tc] = ele[1]["mrt"].std()

    def get_id_upper_and_lower(self):
        for id in self.id2mean.keys():
            self.id2baseline[id] = [get_low_limit(self.sigma, self.id2mean[id], self.id2std[id]),
                                    get_upper_limit(self.sigma, self.id2mean[id], self.id2std[id])]
            print(id + str(self.id2baseline[id]))
        self.dumps()

    def dumps(self):
        baseline_path = "./" + self.data_path.split("/")[-3] + "_" \
                        + self.data_path.split("/")[-1] + "_golddata_baseline.txt"
        with open(baseline_path, "wb") as f:
            pickle.dump(self.id2baseline, f, 1)


if __name__ == '__main__':
    system = "b"
    if system == "a":
        # system-a
        data_path = "../../data/system-a/kpi/kpi_0301.csv"
        test = GOLDDataPreProcessing(data_path, sigma=3)
        test.get_data_from_file()
        test.get_id_upper_and_lower()
    else:
        # system-b
        data_path = "../../data/system-b/kpi/kpi_0130.csv"
        test = GOLDDataPreProcessing(data_path, sigma=3)
        test.get_data_from_file()
        test.get_id_upper_and_lower()
