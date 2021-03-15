#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   simple_threadhold_detection.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/13 12:07 PM   hgh      1.0         None


class SimpleThreadHoldDetection:
    def __init__(self):
        self.stream_kpi = None

    def detect(self, stream_kpi, mrt_threshold):
        self.stream_kpi = stream_kpi
        if self.rr_smaller_100() or self.sr_smaller_100() \
                or self.mrt_detect(mrt_threshold):
            return self.stream_kpi["timestamp"]
        else:
            return None

    def rr_smaller_100(self):
        return self.stream_kpi["rr"] < 100

    def sr_smaller_100(self):
        return self.stream_kpi["sr"] < 100

    def mrt_detect(self, mrt_threshold):
        return self.stream_kpi["mrt"] > mrt_threshold


if __name__ == '__main__':
    msg = {"timestamp": 1614268800, "rr": 100, "sr": 100,
           "count": 106, "mrt": 11, "tc": "aaa51527e6254b04dd1d25004e6da3e6db651e4e"}
    test = SimpleThreadHoldDetection()
    result = test.detect(msg, 100)
    print(result)
