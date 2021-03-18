#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   kpi_producer.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/17 12:47 AM   hgh      1.0         None
import json
import time

from kafka import KafkaProducer

from config import servers


def producer_message(test_data_path, system):
    """
    本地模拟kafka生产者，system-a、b基本通用，通过system关键字来区分
    :param test_data_path:
    :param system: "a" or "b"
    :return:
    """
    producer = KafkaProducer(bootstrap_servers=servers)
    with open(test_data_path, "r") as f:
        i = 0
        for line in f:
            if i > 0:
                lines = line.split(",")
                msg = dict()
                msg["timestamp"] = int(lines[0])
                msg["rr"] = float(lines[1])
                msg["sr"] = float(lines[2])
                if system == "a":
                    msg["count"] = float(lines[3])
                else:
                    msg["cnt"] = float(lines[3])
                msg["mrt"] = float(lines[4])
                msg["tc"] = lines[5].strip()
                print(msg)
                producer.send(system + '-kpi', bytes(json.dumps(msg).encode("utf-8")))
                time.sleep(0.1)
            else:
                i += 1


if __name__ == '__main__':
    system = "a"
    test_data_path = "../data/system-a/kpi/kpi_0226.csv"
    producer_message(test_data_path, system)