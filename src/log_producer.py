#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   log_producer.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/19 4:41 PM   hgh      1.0         None

import json
import time

from kafka import KafkaProducer

from config import servers


def log_producer_message(test_data_path, system):
    producer = KafkaProducer(bootstrap_servers=servers)
    with open(test_data_path, "r") as f:
        i = 0
        header = True
        for line in f:
            if not header:
                lines = line.split(",")
                msg = dict()
                msg["id"] = lines[0]
                msg["timestamp"] = int(lines[1])
                msg["cmdb_id"] = lines[2]
                if system == "a":
                    msg["logname"] = lines[3]
                else:
                    msg["log_name"] = lines[3]
                msg["value"] = lines[4]
                print(msg)
                producer.send(system + '-log', bytes(json.dumps(msg).encode("utf-8")))
                time.sleep(0.15)
                i += 1
                if i > 10:
                    break
            else:
                header = False


if __name__ == '__main__':
    # system a
    # system = "a"
    # log_test_data_path = "../data/system-a/log/log_weblogic_0226.csv"
    # log_producer_message(log_test_data_path, system=system)

    system = "b"
    log_test_data_path = "../data/system-b/log/log_gc_0311.csv"
    log_producer_message(log_test_data_path, system=system)
