#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   trace_producer.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/17 12:06 PM   hgh      1.0         None
import json
import time

from kafka import KafkaProducer

from config import servers


def trace_producer_message(test_data_path, system):
    producer = KafkaProducer(bootstrap_servers=servers)
    with open(test_data_path, "r") as f:
        header = True
        for line in f:
            if not header:
                lines = line.split(",")
                msg = dict()
                if system == "a":
                    msg["cmdb_id"] = lines[1]
                    msg["timestamp"] = int(lines[0])
                    msg["duration"] = float(lines[5])
                else:
                    msg["timestamp"] = int(lines[0])
                    msg["cmdb_id"] = lines[1]
                    msg["parent_id"] = lines[2]
                    msg["span_id"] = lines[3]
                    msg["trace_id"] = lines[4]
                    msg["duration"] = float(lines[5])
                print(msg)
                producer.send(system + '-trace', bytes(json.dumps(msg).encode("utf-8")))
                time.sleep(0.2)
            else:
                header = False


if __name__ == '__main__':
    system = "a"
    if system == "a":
        # system a
        trace_test_data_path = "../data/system-a/trace/trace-0226.csv"
        trace_producer_message(trace_test_data_path, system)
    else:
        # system b
        trace_test_data_path = "../data/system-b/trace/trace_0304.csv"
        trace_producer_message(trace_test_data_path, system)
