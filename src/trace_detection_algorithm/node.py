#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   node.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/17 1:04 PM   hgh      1.0         None


class TraceNode:
    def __init__(self, span_id, cmdb_id, duration):
        self.span_id = span_id
        self.cmdb_id = cmdb_id
        self.duration = duration
        self.childs = []
