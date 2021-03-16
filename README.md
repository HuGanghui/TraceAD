### 关于kafka相关基本概念

主要参考资料：

* [Kafka快速安装](http://www.54tianzhisheng.cn/2018/01/04/Kafka/)

  方便本地测试

* [kafka相关概念及应用实战(Python代码)](https://www.jianshu.com/p/76d4e1eb4882)

  主要介绍kafka的基本概念，以及kafka-python包的一些API的使用，本仓库的kafka-test就是主要参考该文章的

* [kafka-python包的官方文档](https://kafka-python.readthedocs.io/en/master/index.html)

  kafka-python包官方文档，如有需要可以查阅

###  关于python日志的相关基本概念

主要参考资料：

* [python中logging日志模块详解](https://www.cnblogs.com/xianyulouie/p/11041777.html)

* [Python之日志处理（logging模块）](https://www.cnblogs.com/yyds/p/6901864.html)

  两篇博客都有不错的介绍，不过第二个可能更清晰些

* [logging（python3.6官方文档）](https://docs.python.org/3.6/library/logging.html)

----

当前测试效果：

system-a

* 真实异常时间：
  
* [0226真实异常时间](./result/system-a/0226/system-a-0226-fault-time.txt)
  
* 黄金指标异常检测-使用方法：3-Sigma

  检测结果：

  * [golddata_3sigma_no_rr_sr](./result/system-a/0226/golddata_3sigma_no_rr_sr.txt)

    不用rr和sr

  * [golddata_3sigma_and_rr_sr](./result/system-a/0226/golddata_3sigma_and_rr_sr.txt)

    用rr和sr

* trace数据网元异常检测-使用方法：3-Sigma

  * [trace-3sigma](result/system-a/0226/trace_3sigma.txt)

* 黄金指标（不用rr、sr）和trace数据联合测试

  * [gold_3sigma_no_rr_sr_trace_test](result/system-a/0226/gold_3sigma_no_rr_sr_trace_test.txt)

