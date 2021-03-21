#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   log_consumer.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/19 4:33 PM   hgh      1.0         None
import json
import logging
import threading

from kafka import KafkaConsumer

from config import servers
from utils import find_timestamp_key


class LogConsumerThread(threading.Thread):
    def __init__(self, name, ne2ts2logs, system):
        threading.Thread.__init__(self)
        self.name = name
        self.system = system

        logging.basicConfig(level=logging.INFO)
        self.log_consumer_logger = logging.getLogger("log_consumer_" + system)
        log_consumer_handler = logging.FileHandler("log_consumer_" + system + ".log")
        log_consumer_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        self.log_consumer_logger.addHandler(log_consumer_handler)

        # TODO 考虑到线程安全问题，因此容器需要保证线程安全
        self.ne2ts2logs = ne2ts2logs
        self.log_consumer = KafkaConsumer(system + '-log',
                                          bootstrap_servers=servers,
                                          auto_offset_reset='latest',
                                          enable_auto_commit=False,
                                          security_protocol='PLAINTEXT')

    def run(self):
        self.log_consumer_logger.info("开始线程：" + self.name)
        self.consume()
        self.log_consumer_logger.info("退出线程：" + self.name)

    def consume(self):
        i = 0
        for message in self.log_consumer:
            log_data = json.loads(message.value.decode('utf8'))
            if i % 10000 == 0:
                self.log_consumer_logger.info(log_data)
            if self.system == "a":
                self.save_logs_a(log_data)
            else:
                self.save_logs_b(log_data)
            i += 1

    def save_logs_a(self, log_data):
        ne = log_data["cmdb_id"]
        ts = find_timestamp_key(log_data["timestamp"])
        old_ts = ts - 60
        try:
            log_name = log_data["logname"]
        except KeyError:
            log_name = log_data["log_name"]
        try:
            log_id = log_data["id"]
        except KeyError:
            log_id = log_data["log_id"]
        logs = log_id + "," + str(ts) + "," + ne + "," + log_name + "," + log_data["value"]
        if ne not in self.ne2ts2logs:
            self.ne2ts2logs[ne] = dict()
        if ts not in self.ne2ts2logs[ne]:
            self.ne2ts2logs[ne][ts] = dict()
        if log_name not in self.ne2ts2logs[ne][ts]:
            self.ne2ts2logs[ne][ts][log_name] = []
        # 进行淘汰工作
        if old_ts in self.ne2ts2logs[ne]:
            del self.ne2ts2logs[ne][old_ts]
        self.ne2ts2logs[ne][ts][log_name].append(logs)

    def save_logs_b(self, log_data):
        log_name = log_data["log_name"]
        if log_name == "gc":
            ne = log_data["cmdb_id"]
            ts = find_timestamp_key(log_data["timestamp"])
            old_ts = ts - 60
            logs = log_data["log_id"] + "," + str(ts) + "," + ne + "," + log_name + "," + log_data["value"]
            if ne not in self.ne2ts2logs:
                self.ne2ts2logs[ne] = dict()
            if ts not in self.ne2ts2logs[ne]:
                self.ne2ts2logs[ne][ts] = dict()
            if log_name not in self.ne2ts2logs[ne][ts]:
                self.ne2ts2logs[ne][ts][log_name] = []
            # 进行淘汰工作
            if old_ts in self.ne2ts2logs[ne]:
                del self.ne2ts2logs[ne][old_ts]
            self.ne2ts2logs[ne][ts][log_name].append(logs)


if __name__ == '__main__':
    ne2ts2logs = dict()
    system = "b"
    thread1 = LogConsumerThread("Thread-log-consumer", ne2ts2logs, system)
    # 开启新线程
    thread1.start()
    thread1.join()
    print("退出主线程")