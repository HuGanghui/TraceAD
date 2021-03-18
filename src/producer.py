#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   producer.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/17 8:25 PM   hgh      1.0         None
import _thread

from kpi_producer import producer_message
from metric_producer import metric_producer_message
from trace_producer import trace_producer_message

if __name__ == '__main__':
    system = "a"

    if system == "a":
        # system a
        test_data_path = "../data/system-a/kpi/kpi_0226.csv"
        _thread.start_new_thread(producer_message, (test_data_path, system,))

        trace_test_data_path = "../data/system-a/trace/trace-0226.csv"
        _thread.start_new_thread(trace_producer_message, (trace_test_data_path,))

        # system a
        trace_test_data_path = "../data/system-a/metric/metric_0226.csv"
        _thread.start_new_thread(metric_producer_message, (trace_test_data_path, system,))

    while 1:
        pass
