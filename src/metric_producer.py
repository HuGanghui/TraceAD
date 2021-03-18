#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   metric_producer.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/17 7:02 PM   hgh      1.0         None

import json
import time

from kafka import KafkaProducer

from config import servers


def metric_producer_message(test_data_path, system):
    producer = KafkaProducer(bootstrap_servers=servers)
    with open(test_data_path, "r") as f:
        header = True
        for line in f:
            if not header:
                lines = line.split(",")
                msg = dict()
                msg["timestamp"] = int(lines[0])
                msg["cmdb_id"] = lines[1]
                msg["kpi_name"] = lines[2]
                msg["value"] = float(lines[3])
                # print(msg)
                producer.send(system + '-metric', bytes(json.dumps(msg).encode("utf-8")))
                time.sleep(0.15)

            else:
                header = False


if __name__ == '__main__':
    # system a
    trace_test_data_path = "../data/system-a/metric/metric_0226.csv"
    metric_producer_message(trace_test_data_path, system="a")
