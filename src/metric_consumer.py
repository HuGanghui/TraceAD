#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   metric_comsumer.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/17 5:49 PM   hgh      1.0         None
import json
import logging
import threading

from kafka import KafkaConsumer

from config import servers
from utils import find_timestamp_key


class MetricConsumerThread(threading.Thread):
    def __init__(self, name, ne2ts2metrics, system):
        threading.Thread.__init__(self)
        self.name = name
        self.system = system

        logging.basicConfig(level=logging.INFO)
        self.metric_consumer_logger = logging.getLogger("metric_consumer_" + system)
        metric_consumer_handler = logging.FileHandler("metric_consumer_" + system + ".log")
        metric_consumer_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        self.metric_consumer_logger.addHandler(metric_consumer_handler)

        # TODO 考虑到线程安全问题，因此容器需要保证线程安全
        self.ne2ts2metrics = ne2ts2metrics
        self.kpi_consumer = KafkaConsumer(system + '-metric',
                                          bootstrap_servers=servers,
                                          auto_offset_reset='latest',
                                          enable_auto_commit=False,
                                          security_protocol='PLAINTEXT')

    def run(self):
        self.metric_consumer_logger.info("开始线程：" + self.name)
        self.consume()
        self.metric_consumer_logger.info("退出线程：" + self.name)

    def consume(self):
        i = 0
        for message in self.kpi_consumer:
            metric_data = json.loads(message.value.decode('utf8'))
            # print(metric_data)
            self.save_metric(metric_data)
            i += 1
            if i % 10000 == 0:
                self.metric_consumer_logger.info(metric_data)

    def save_metric(self, metric_data):
        ne = metric_data["cmdb_id"]
        ts = find_timestamp_key(metric_data["timestamp"])
        old_ts = ts - 300 * 3
        kpi_name = metric_data["kpi_name"]
        value = metric_data["value"]
        if ne not in self.ne2ts2metrics:
            self.ne2ts2metrics[ne] = dict()
        if ts not in self.ne2ts2metrics[ne]:
            self.ne2ts2metrics[ne][ts] = dict()
        if kpi_name not in self.ne2ts2metrics[ne][ts]:
            self.ne2ts2metrics[ne][ts][kpi_name] = []
        # 进行淘汰工作
        if old_ts in self.ne2ts2metrics[ne]:
            del self.ne2ts2metrics[ne][old_ts]
        self.ne2ts2metrics[ne][ts][kpi_name].append(value)


if __name__ == '__main__':
    ne2ts2metrics = dict()
    system = "a"
    thread1 = MetricConsumerThread("Thread-trace-consumer", ne2ts2metrics, system)
    # 开启新线程
    thread1.start()
    thread1.join()
    print("退出主线程")

