# storm-alarm
## 系统基本架构
整个日志预警系统的架构就是先由反向代理业务系统的网关服务器产生网关日志, 然后使用Flume去监听网关日志，并实时把每一条日志信息抓取下来并存进Kafka消息系统中, 接着由Storm系统消费Kafka中的消息，根据规则存储到MongoDB中。
