#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   metric_consumer.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/13 10:03 PM   hgh      1.0         None
from __future__ import absolute_import

import json
import logging
from copy import copy

from kafka import KafkaConsumer
import requests

from detection.metric_simple_detection import MetricSimpleDetection

system = "a"

logging.basicConfig(level=logging.INFO)
consumer_logger = logging.getLogger("consumer")
consumer_handler = logging.FileHandler("metric_consumer_" + system + ".log")
consumer_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
consumer_logger.addHandler(consumer_handler)

# AVAILABLE_TOPICS = set(['a-kpi', 'a-metric', 'a-trace', 'a-log'])
# CONSUMER = KafkaConsumer(system + '-metric',
#                          bootstrap_servers=['10.3.2.41', '10.3.2.4', '10.3.2.36'],
#                          auto_offset_reset='latest',
#                          enable_auto_commit=False,
#                          security_protocol='PLAINTEXT')

# 本地测试
CONSUMER = KafkaConsumer('metric-json-test',
                         bootstrap_servers=['localhost'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         security_protocol='PLAINTEXT')


def submit(ctx):
    assert (isinstance(ctx, list))
    for tp in ctx:
        assert(isinstance(tp, list))
        assert(len(tp) == 2)
        assert(isinstance(tp[0], str))
        assert(isinstance(tp[1], str) or (tp[1] is None))
    data = {'content': json.dumps(ctx)}
    r = requests.post('http://10.3.2.25:5000/standings/submit/', data=json.dumps(data))
    return r.text


def main():
    '''Consume data and react'''
    # assert AVAILABLE_TOPICS <= CONSUMER.topics(), 'Please contact admin'
    metric_dict = {}
    test = MetricSimpleDetection(system)
    consumer_logger.info("start consume")
    for message in CONSUMER:
        # if message.topic == 'a-metric':
        if message.topic == 'metric-json-test':
            data = json.loads(message.value.decode('utf8'))
            key = data["cmdb_id"] + "_" + data["kpi_name"]
            if key in metric_dict:
                key_list = metric_dict[key]
                if len(key_list) == 5:
                    res_kpi_name = test.metric_test([copy(key_list)], key)
                    if len(res_kpi_name) != 0:
                        result = [data["cmdb_id"], data["kpi_name"]]
                        consumer_logger.info("detected anomaly")
                        submit(result)
                        consumer_logger.info("anomaly: " + str(result))
                    else:
                        consumer_logger.info("good: " + key)
                    key_list.pop(0)
                    key_list.append(data["value"])
                else:
                    key_list.append(data["value"])

            else:
                metric_dict[key] = []
                metric_dict[key].append(data["value"])


if __name__ == '__main__':
    # if sys.argv[1] == "submit":
    #     # test part
    #     while True:
    #         r = submit([["docker_003", "container_cpu_used"]])
    #         time.sleep(60)
    # elif sys.argv[1] == "consume":
    #     # start to consume kafka
    #     main()

    main()


