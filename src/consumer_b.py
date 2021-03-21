#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   consumer_b.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/18 4:55 PM   hgh      1.0         None
import _thread
import json
import logging
import os
import queue
import time
from copy import copy

import requests

from log_consumer import LogConsumerThread
from log_detection_algorithm.real_time_log_anomaly_detection_demo import ai_ops_log_anomaly_detection
from metric_detection_algorithm.detection.test_rank import MetricSimpleDetection
from kpi_consumer import KPIConsumerThread
from metric_consumer import MetricConsumerThread
from trace_consumer import TraceConsumerThread
from trace_detection_algorithm.trace_simple_detection_b import TraceSimpleDetectionB
from utils import find_timestamp_key

logging.basicConfig(level=logging.INFO)
consumer_logger = logging.getLogger("consumer_b")
consumer_handler = logging.FileHandler("consumer_" + "b" + ".log")
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
    r = requests.post('http://10.3.2.25:5001/standings/submit/', data=json.dumps(data))
    return r.text


def main_b():
    system = "b"
    # 黄金指标
    gold_data_ad_q = queue.Queue()
    cur_path = os.path.abspath(os.path.dirname(__file__))
    gold_data_system_b_baseline_path = cur_path \
                                       + "/golddata_detection_algorithm/system-b_kpi_0130.csv_golddata_baseline.txt"
    kpi_thread = KPIConsumerThread("Thread-kpi-consumer", gold_data_system_b_baseline_path, gold_data_ad_q, system)
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

    # log数据
    ne2ts2logs = dict()
    log_thread = LogConsumerThread("Thread-log-consumer", ne2ts2logs, system)
    # 开启新线程
    log_thread.start()

    # 检测线程
    _thread.start_new_thread(start_detect, (gold_data_ad_q, ts2traces, ne2ts2metrics, ne2ts2logs))

    kpi_thread.join()
    trace_thread.join()
    metric_thread.join()
    log_thread.join()


def start_detect(q, ts2traces, ne2ts2metrics, ne2ts2logs):
    cur_path = os.path.abspath(os.path.dirname(__file__))
    trace_baseline_path = cur_path + "/trace_detection_algorithm/system-b_trace_0311.csv_22266996_trace_baseline.txt"
    trace_model = TraceSimpleDetectionB(trace_baseline_path, ts2traces)
    # TODO 暂时先用，后续可能会用更合适的模型
    metric_model = MetricSimpleDetection("b")
    consumer_logger.info('start get q')
    while True:
        try:
            ad_ts = q.get()
            consumer_logger.info("ad ts: " + str(ad_ts))
            strtime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(ad_ts))
            consumer_logger.info(strtime)
            trace_result = trace_model.detect_b(ad_ts)
            if len(trace_result) == 0:
                consumer_logger.info("in " + str(ad_ts) + " no trace error")
            else:
                ne_ad = trace_result[0]
                consumer_logger.info("trace ad: " + str(ne_ad))
                consumer_logger.info("start metric ad")
                ts_key = find_timestamp_key(ad_ts)
                answer = []
                get_metric_detect_result(ne_ad, ts_key, metric_model, ne2ts2metrics, answer, consumer_logger)
                get_log_detect_result(ne_ad, ts_key, ne2ts2logs, answer, consumer_logger)
                if len(trace_result) > 2:
                    consumer_logger.info("trace ad2: " + str(ne_ad))
                    consumer_logger.info("start metric ad")
                    ne_ad2 = trace_result[1]
                    get_metric_detect_result(ne_ad2, ts_key, metric_model, ne2ts2metrics, answer, consumer_logger)
                    get_log_detect_result(ne_ad2, ts_key, ne2ts2logs, answer, consumer_logger)
                if len(answer) == 0:
                    consumer_logger.info("in " + str(ad_ts) + " and trace_ad: " + str(ne_ad) + " no metric/log error")
                else:
                    submit_response = submit(answer)
                    consumer_logger.info("submit_response: " + submit_response)
                    consumer_logger.info("answer: " + str(answer))
        except Exception as e:
            consumer_logger.error("exception happened: " + str(e) + " " + str(e.args))
            continue


def get_metric_detect_result(ne_ad, ts_key, metric_model, ne2ts2metrics, answer, con_logger):
    metric_datas = []
    kpi_names = []
    if ne_ad in list(ne2ts2metrics.keys()) and ts_key in list(ne2ts2metrics[ne_ad].keys()):
        for ele in list(ne2ts2metrics[ne_ad][ts_key].keys()):
            metric_datas.append(copy(ne2ts2metrics[ne_ad][ts_key][ele]))
            kpi_names.append(ele)
        # TODO metric detect 有bug，空的输进去也是异常
        metric_results = metric_model.metric_test(metric_datas, ne_ad, kpi_names)
        if len(metric_results) == 0:
            con_logger.info("in " + str(ts_key) + " and trace_ad: " + str(ne_ad) + " no metric ad")
        else:
            con_logger.info("metric ad: " + str(metric_results))
        for ele in metric_results:
            answer.append([ne_ad, ele])
    else:
        con_logger.info("ne_ad or ts_key not in ne2ts2metrics")


def get_log_detect_result(ne_ad, ts_key, ne2ts2logs, answer, con_logger):
    log_results = []
    log_datas_list = []
    if ne_ad in list(ne2ts2logs.keys()) and ts_key in list(ne2ts2logs[ne_ad].keys()):
        for ele in list(ne2ts2logs[ne_ad][ts_key].keys()):
            log_datas = ne2ts2logs[ne_ad][ts_key][ele]
            if len(log_datas) != 0:
                log_datas_list.append(log_datas)
        for log_datas in log_datas_list:
            log_result = ai_ops_log_anomaly_detection(log_datas)
            if log_result != "Normal":
                log_results.append(log_result)
                con_logger.info("log ad: " + str(log_results))
        for ele in log_results:
            answer.append([ne_ad, ele])
    else:
        con_logger.info("ne_ad or ts_key not in ne2ts2logs")


def get_metric_detect_result_test():
    ne_ad = "gjjcore2"
    ts_key = find_timestamp_key(1614290340)
    metric_model = MetricSimpleDetection("a")
    ne2ts2metrics = dict()
    ne2ts2metrics["gjjcore2"] = dict()
    ne2ts2metrics["gjjcore2"][ts_key] = dict()
    ne2ts2metrics["gjjcore2"][ts_key]["test"] = []
    answer = []
    get_metric_detect_result(ne_ad, ts_key, metric_model, ne2ts2metrics, answer, consumer_logger)


def get_log_detect_result_test():
    ne_ad = "gjjcore2"
    ts_key = find_timestamp_key(1614290340)
    ne2ts2logs = dict()
    ne2ts2logs["gjjcore2"] = dict()
    ne2ts2logs["gjjcore2"][ts_key] = dict()
    ne2ts2logs["gjjcore2"][ts_key]["test"] = []
    answer = []
    get_log_detect_result(ne_ad, ts_key, ne2ts2logs, answer, consumer_logger)


if __name__ == '__main__':
    # TODO 后续需要完成测试
    # test
    # get_metric_detect_result_test()
    # get_log_detect_result_test()
    main_b()
    consumer_logger.info("退出主线程")

