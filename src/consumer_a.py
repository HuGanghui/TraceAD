#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   consumer.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/13 1:27 PM   hgh      1.0         None
import _thread
import json
import logging
import os
import queue
import time
from copy import copy

import requests

from metric_detection_algorithm.detection.metric_simple_detection import MetricSimpleDetection
from kpi_consumer import KPIConsumerThread
from metric_consumer import MetricConsumerThread
from trace_consumer import TraceConsumerThread
from trace_detection_algorithm.trace_simple_detection import TraceSimpleDetection
from utils import find_timestamp_key

logging.basicConfig(level=logging.INFO)
consumer_logger = logging.getLogger("consumer")
consumer_handler = logging.FileHandler("consumer_" + "a" + ".log")
consumer_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
consumer_logger.addHandler(consumer_handler)


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


# def main():
#     '''Consume data and react'''
#     # assert AVAILABLE_TOPICS <= CONSUMER.topics(), 'Please contact admin'
#     for message in CONSUMER:
#         if message.topic == 'kpi-json-test':
#             data = json.loads(message.value.decode('utf8'))
#             test = SimpleThreadHoldDetection()
#             result = test.detect(data, 100)
#             print(result)


def main_a():
    system = "a"
    # 黄金指标
    gold_data_ad_q = queue.Queue()
    curPath = os.path.abspath(os.path.dirname(__file__))
    gold_data_system_a_baseline_path = curPath + "/golddata_detection_algorithm/system-a_kpi_0301.csv_golddata_baseline.txt"
    kpi_thread = KPIConsumerThread("Thread-kpi-consumer", gold_data_system_a_baseline_path, gold_data_ad_q, system)
    # 开启新线程
    kpi_thread.start()

    # # Trace数据
    ts2traces = dict()
    trace_thread = TraceConsumerThread("Thread-trace-consumer", ts2traces, system)
    # 开启新线程
    trace_thread.start()
    #
    # # Metric数据
    ne2ts2metrics = dict()
    metric_thread = MetricConsumerThread("Thread-metric-consumer", ne2ts2metrics, system)
    # 开启新线程
    metric_thread.start()

    # 检测线程
    _thread.start_new_thread(start_detect, (gold_data_ad_q, ts2traces, ne2ts2metrics))

    kpi_thread.join()
    trace_thread.join()
    metric_thread.join()


def start_detect(q, ts2traces, ne2ts2metrics):
    curPath = os.path.abspath(os.path.dirname(__file__))
    trace_baseline_path = curPath + "/trace_detection_algorithm/system-a_trace-0227.csv_trace_baseline.txt"
    trace_model = TraceSimpleDetection(trace_baseline_path, ts2traces)
    metric_model = MetricSimpleDetection("a")
    consumer_logger.info('start get q')
    while True:
        try:
            ad_ts = q.get()
            consumer_logger.info("ad ts: " + str(ad_ts))
            strtime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(ad_ts))
            consumer_logger.info(strtime)
            trace_result = trace_model.detect(ad_ts)
            if len(trace_result) == 0:
                consumer_logger.info("in " + str(ad_ts) + " no trace error")
            else:
                ne_ad = trace_result[0]
                consumer_logger.info("trace ad: " + str(ne_ad))
                metric_datas = []
                kpi_names = []
                consumer_logger.info("start metric ad")
                ts_key = find_timestamp_key(ad_ts)
                if ne_ad in ne2ts2metrics and ts_key in ne2ts2metrics:
                    for ele in ne2ts2metrics[ne_ad][ts_key]:
                        metric_datas.append(copy(ne2ts2metrics[ne_ad][ad_ts][ele]))
                        kpi_names.append(ele)
                    metric_results = metric_model.metric_test(metric_datas, ne_ad, kpi_names)
                    consumer_logger.info("metric ad: " + metric_results)
                    answer = []
                    for ele in metric_results:
                        answer.append([ne_ad, ele])
                    if len(answer) == 0:
                        consumer_logger.info("in " + str(ad_ts) + " and trace_ad: " + str(ne_ad) + " no metric error")
                    else:
                        submit(answer)
                        consumer_logger.info("answer: " + str(answer))
                else:
                    consumer_logger.info("ne_ad or ts_key not in ne2ts2metrics")
        except Exception as e:
            consumer_logger.error("exception happened: " + str(e) + " " + str(e.args))
            continue


if __name__ == '__main__':
    # if sys.argv[1] == "submit":
    #     # test part
    #     while True:
    #         r = submit([["docker_003", "container_cpu_used"]])
    #         time.sleep(60)
    # elif sys.argv[1] == "consume":
    #     # start to consume kafka
    #     main()
    # TODO 后续需要完成测试
    main_a()
    consumer_logger.info("退出主线程")
