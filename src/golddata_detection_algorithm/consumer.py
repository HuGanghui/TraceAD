#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   consumer.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/13 1:27 PM   hgh      1.0         None
import json
import sys
import time

from kafka import KafkaConsumer
import requests

from golddata_detection_algorithm import SimpleThreadHoldDetection

AVAILABLE_TOPICS = set(['a-kpi', 'a-metric', 'a-trace', 'a-log'])
# CONSUMER = KafkaConsumer('a-kpi',
#                          bootstrap_servers=['10.3.2.41', '10.3.2.4', '10.3.2.36'],
#                          auto_offset_reset='latest',
#                          enable_auto_commit=False,
#                          security_protocol='PLAINTEXT')

# 本地测试
CONSUMER = KafkaConsumer('kpi-json-test',
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
    for message in CONSUMER:
        if message.topic == 'kpi-json-test':
            data = json.loads(message.value.decode('utf8'))
            test = SimpleThreadHoldDetection()
            result = test.detect(data, 100)
            print(result)


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
