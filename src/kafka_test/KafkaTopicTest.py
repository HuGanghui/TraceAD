#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   KafkaTopicTest.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/10 8:44 PM   hgh      1.0         None

from kafka import KafkaConsumer, TopicPartition
from kafka_test.config import servers


# 获取topic列表以及topic的分区列表
def retrieve_topics():
    consumer = KafkaConsumer(bootstrap_servers=servers)
    print(consumer.topics())


# 获取topic的分区列表
def retrieve_partitions(topic):
    consumer = KafkaConsumer(bootstrap_servers=servers)
    print(consumer.partitions_for_topic(topic))


# 获取Consumer Group对应的分区的当前偏移量
def retrieve_partition_offset():
    consumer = KafkaConsumer(bootstrap_servers=servers,
                             group_id='kafka-group-id')
    tp = TopicPartition('kafka-topic', 0)
    consumer.assign([tp])
    print("starting offset is ", consumer.position(tp))


if __name__ == '__main__':
    retrieve_topics()
    # retrieve_partitions("kafka-topic")
    # retrieve_partition_offset()