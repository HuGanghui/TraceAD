#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   trace_consumer.py.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/17 11:56 AM   hgh      1.0         None

import json
import logging
import threading

from kafka import KafkaConsumer

from config import servers
from utils import find_timestamp_key


class TraceConsumerThread(threading.Thread):
    def __init__(self, name, ts2traces, system):
        threading.Thread.__init__(self)
        self.name = name
        self.system = system

        logging.basicConfig(level=logging.INFO)
        self.trace_consumer_logger = logging.getLogger("trace_consumer_" + system)
        trace_consumer_handler = logging.FileHandler("trace_consumer_" + system + ".log")
        trace_consumer_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        self.trace_consumer_logger.addHandler(trace_consumer_handler)

        # TODO 考虑到线程安全问题，因此容器需要保证线程安全
        self.timestamp2traces = ts2traces
        self.kpi_consumer = KafkaConsumer(system + '-trace',
                                          bootstrap_servers=servers,
                                          auto_offset_reset='latest',
                                          enable_auto_commit=False,
                                          security_protocol='PLAINTEXT')

    def run(self):
        self.trace_consumer_logger.info("开始线程：" + self.name)
        self.consume()
        self.trace_consumer_logger.info("退出线程：" + self.name)

    def consume(self):
        i = 0
        for message in self.kpi_consumer:
            trace_data = json.loads(message.value.decode('utf8'))
            if self.system == "a":
                self.save_trace_a(trace_data)
            else:
                self.save_trace_b(trace_data)
            i += 1
            if i % 10000 == 0:
                self.trace_consumer_logger.info(message)

    def save_trace_a(self, trace_data):
        """
        针对a系统的traces都是只有一条，因此直接保存该网元以及对应的duration，后续用来判断是否异常
        :param trace_data: 单条trace数据
        :return:
        """
        ts = find_timestamp_key(trace_data["timestamp"])
        old_ts = ts - 300 * 3
        if ts not in self.timestamp2traces:
            self.timestamp2traces[ts] = []
        # 删除过期数据
        if old_ts in self.timestamp2traces:
            del self.timestamp2traces[old_ts]
        self.timestamp2traces[ts].append((trace_data["cmdb_id"], trace_data["duration"]))

    def save_trace_b(self, trace_data):
        ts = find_timestamp_key(trace_data["timestamp"])
        old_ts = ts - 300 * 3
        if ts not in self.timestamp2traces:
            self.timestamp2traces[ts] = dict()
        if trace_data["trace_id"] not in self.timestamp2traces[ts]:
            self.timestamp2traces[ts][trace_data["trace_id"]] = []
        # 删除过期数据
        if old_ts in self.timestamp2traces:
            del self.timestamp2traces[old_ts]
        # TODO 是不是无需字典
        self.timestamp2traces[ts][trace_data["trace_id"]].append({"cmdb_id": trace_data["cmdb_id"],
                                                                  "parent_id": trace_data["parent_id"],
                                                                  "span_id": trace_data["span_id"],
                                                                  "duration": trace_data["duration"]})


if __name__ == '__main__':
    ts2traces = dict()
    system = "b"
    thread1 = TraceConsumerThread("Thread-trace-consumer", ts2traces, system)
    # 开启新线程
    thread1.start()
    thread1.join()
    print("退出主线程")
