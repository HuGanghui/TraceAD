#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   KafkaProducerTest.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/10 8:29 PM   hgh      1.0         None

from kafka import KafkaProducer
from kafka_test.config import servers
from kafka_test.LogConfig import logging

logger = logging.getLogger("producer")
f_handler = logging.FileHandler('producer.log')
f_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(f_handler)


def producer_message():
    producer = KafkaProducer(bootstrap_servers=servers)
    for i in range(10):
        msg = "a-log message " + str(i+200)
        # topic 和 value
        producer.send('a-log', bytes(msg.encode("utf-8")))
        # print(msg)
        logger.info(msg)

    for i in range(10):
        msg = "a-kpi message " + str(i+500)
        # topic 和 value
        producer.send('a-kpi', bytes(msg.encode("utf-8")))
        # print(msg)
        logger.info(msg)


if __name__ == '__main__':
    producer_message()