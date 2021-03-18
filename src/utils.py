#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   utils.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/12 5:59 PM   hgh      1.0         None
import time
import numpy as np


def strtime2timestamp(data_path):
    with open(data_path) as f:
        for line in f:
            line = line.strip()
            # print(line)
            try:
                timestamp = int(time.mktime(time.strptime(line, '%Y/%m/%d %H:%M:%S')))
            except ValueError:
                timestamp = int(time.mktime(time.strptime(line, '%Y/%m/%d %H:%M')))
            print(timestamp, end=",")
            # print(time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(timestamp)))


def is_array(data):
    """
    判断数据是否是ndarray格式，如果不是则将数据转化为array类型
    :param data:
    :return: data numpy.array
    """
    if data is not np.ndarray:
        data = np.asarray(data)
    return data


def get_data_deviation(data, mean, window_len):
    """
    计算数据的标准差
    :param data: 数据
    :param mean: 均值
    :param window_len: 滑窗长度
    :return: 标准差
    """
    data = is_array(data)
    data_var = np.sum((data - mean) ** 2 / window_len)
    data_deviation = np.sqrt(data_var)
    return data_deviation


def get_low_limit(sigma, mean, standard_deviation):
    """
    计算动态基线的下限，利用使用3Sigma原则
    :param mean: float 均值
    :param standard_deviation: float 标准差
    :return:
    """
    return mean - sigma * standard_deviation


def get_upper_limit(sigma, mean, standard_deviation):
    """
    计算动态基线的上限，利用使用3Sigma原则
    :param mean: 均值
    :param standard_deviation: 标准差
    :return:
    """
    return mean + sigma * standard_deviation


def find_timestamp_key(anomaly_timestamp):
    """
    由于时间戳可以被300整除，因此通过减去余数可以直接获得anomaly_timestamp的区间ts_key，
    这里以300秒为一个区间

    :param anomaly_timestamp:
    :return: 最后一个小于anomaly_timestamp的ts，也就是其所属的区间
    """
    ts_len = len(str(anomaly_timestamp))
    if ts_len == 10:
        timestamp_key = anomaly_timestamp - anomaly_timestamp % 300
    else:
        anomaly_timestamp = int(anomaly_timestamp / pow(10, ts_len-10))
        timestamp_key = anomaly_timestamp - anomaly_timestamp % 300
    return timestamp_key


if __name__ == '__main__':
    # # system a
    # data_path = "../data/system-a/system-a-0226-fault-time.txt"
    # # system b
    # data_path = "../data/system-b/system-b-0304-fault-time.txt"
    # strtime2timestamp(data_path)
    print(find_timestamp_key(1614787199628))
