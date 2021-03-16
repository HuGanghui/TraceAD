#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   utils.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/12 5:59 PM   hgh      1.0         None
import time


def strtime2timestamp(data_path):
    with open(data_path) as f:
        for line in f:
            line = line.strip()
            print(line)
            timestamp = int(time.mktime(time.strptime(line, '%Y/%m/%d %H:%M:%S')))
            print(timestamp)
            print(time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(timestamp)))


if __name__ == '__main__':
    data_path = "../../data/system-a/system-a-0226-fault-time.txt"
    strtime2timestamp(data_path)
