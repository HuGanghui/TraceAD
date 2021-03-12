#!/usr/bin/env python
# -*- coding:utf-8 -*-

# @File    :   KafkaConsumerTest.py    
# @Contact :   19120364@bjtu.edu.com

# @Modify Time      @Author    @Version    @Description
# ------------      -------    --------    -----------
# 2021/3/10 8:34 PM   hgh      1.0         None

from kafka import KafkaConsumer
from kafka import TopicPartition
from kafka_test.config import servers


# 消费方式1，默认消费所有分区的消息
def consumer_message1():
    consumer = KafkaConsumer('test',
                             bootstrap_servers=servers,
                             group_id="kafka-group-id1",
                             auto_offset_reset="earliest")
    for msg in consumer:
        print("consumer_message1 " + msg.value.decode("utf-8") + " " + str(msg))


def consumer_message1_():
    consumer = KafkaConsumer('test',
                             bootstrap_servers=servers,
                             group_id="kafka-group-id1_",
                             auto_offset_reset="earliest")
    for msg in consumer:
        print("consumer_message1_ " + msg.value.decode("utf-8") + " " + str(msg))


# 消费方式2, 指定消费分区
def consumer_message2():
    consumer = KafkaConsumer(bootstrap_servers=servers,
                             group_id="test")
    consumer.assign([TopicPartition('kafka-topic', 0)])
    for msg in consumer:
        print(msg)


# 消费方式3，手动commit，生产中建议使用这种方式
def consumer_message3():
    consumer = KafkaConsumer(bootstrap_servers=servers,
                             consumer_timeout_ms=1000,
                             group_id="kafka-group-id",
                             enable_auto_commit=False)
    consumer.assign([TopicPartition('kafka-topic', 0)])
    for msg in consumer:
        print(msg)
        consumer.commit()


def block_test():
    # 进行阻塞测试，确实因为第一个consumer有for循环，因此第二个无法消费到数据
    consumer_message1_()
    consumer_message1()


if __name__ == '__main__':
    block_test()