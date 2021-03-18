#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   KpiComsumer.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/17 12:15 AM   hgh      1.0         None
import _thread
import json
import logging
import queue
import threading
import time

from kafka import KafkaConsumer

from config import servers
from golddata_detection_algorithm import SimpleThreadHoldDetection


logging.basicConfig(level=logging.INFO)
kpi_consumer_logger = logging.getLogger("kpi_consumer")
kpi_consumer_handler = logging.FileHandler("kpi_consumer_" + "a" + ".log")
kpi_consumer_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
kpi_consumer_logger.addHandler(kpi_consumer_handler)


class KPIConsumerThread(threading.Thread):
    def __init__(self, name, baseline_path, q, system):
        threading.Thread.__init__(self)
        self.name = name
        self.q = q
        self.gold_ad_model = SimpleThreadHoldDetection(baseline_path)

        self.kpi_consumer = KafkaConsumer(system + '-kpi',
                                          bootstrap_servers=servers,
                                          auto_offset_reset='latest',
                                          enable_auto_commit=False,
                                          security_protocol='PLAINTEXT')

    def run(self):
        kpi_consumer_logger.info("开始线程：" + self.name)
        self.consume()
        kpi_consumer_logger.info("退出线程：" + self.name)

    def consume(self):
        prev_ts = None
        for message in self.kpi_consumer:
            kpi_data = json.loads(message.value.decode('utf8'))
            # print(kpi_data)
            ad_ts = self.gold_ad_model.detect(kpi_data)
            # TODO 增加逻辑：如果是相同或者相近的ts就没必要放进去
            if ad_ts is not None:
                # 避免过度报警
                if prev_ts is None or ad_ts > (prev_ts + 180):
                    prev_ts = ad_ts
                    self.q.put(ad_ts)
                    kpi_consumer_logger.info("put: " + str(ad_ts))
                    strtime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(ad_ts))
                    kpi_consumer_logger.info(strtime)
                else:
                    kpi_consumer_logger.info("ad but no put: " + str(ad_ts))
                    strtime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(ad_ts))
                    kpi_consumer_logger.info(strtime)


def get_q_ele(q):
    print('start get q')
    while True:
        print("get: " + str(q.get()))
        # time.sleep(0.001)


if __name__ == '__main__':
    q = queue.Queue()
    baseline_path = "./golddata_detection_algorithm/system-a_kpi_0301.csv_golddata_baseline.txt"

    # 创建新线程
    _thread.start_new_thread(get_q_ele, (q,))
    thread1 = KPIConsumerThread("Thread-kpi-consumer", baseline_path, q)
    # 开启新线程
    thread1.start()
    thread1.join()
    # get_q_ele(q)
    print("退出主线程")
