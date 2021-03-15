#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   KafkaProducerTest.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/10 8:29 PM   hgh      1.0         None
import json

from kafka import KafkaProducer
from kafka_test.config import servers
from kafka_test.LogConfig import logging

logger = logging.getLogger("producer")
f_handler = logging.FileHandler('producer.log')
f_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(f_handler)


def producer_message():
    producer = KafkaProducer(bootstrap_servers=servers)
    # for i in range(10):
    #     # msg = {"timestamp": 1614268800, "rr": 100, "sr": 100,
    #     #        "count":106, "mrt": 11, "tc": "aaa51527e6254b04dd1d25004e6da3e6db651e4e"}
    #     msg = "a-log message " + str(i+200)
    #     # topic 和 value
    #     producer.send('a-log', bytes(msg.encode("utf-8")))
    #     # print(msg)
    #     logger.info(msg)

    # kpi test
    # for i in range(10):
    #     msg = {"timestamp": 1614268800, "rr": 100, "sr": 100,
    #            "count": 106, "mrt": 110, "tc": "aaa51527e6254b04dd1d25004e6da3e6db651e4e"}
    #     # msg = "a-kpi message " + str(i+500)
    #     # topic 和 value
    #     producer.send('kpi-json-test', bytes(json.dumps(msg).encode("utf-8")))
    #     # print(msg)
    #     logger.info(msg)

    # metric test
    for i in range(10):
        msg = {"timestamp": 1614268800, "cmdb_id": "gjjweb001",
               "kpi_name": "system.cpu.guest", "value": 0.0}
        # msg = "a-kpi message " + str(i+500)
        # topic 和 value
        producer.send('metric-json-test', bytes(json.dumps(msg).encode("utf-8")))
        # print(msg)
        logger.info(msg)


if __name__ == '__main__':
    producer_message()