package test.icetea.rocketmq.client;

public class ServerConfig {

    // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
    public static final String ENDPOINTS = "192.168.136.128:8081";
    // 普通消息topic
    public static final String NORMAL_TOPIC_NAME = "test_topic";
    // 延迟消息topic
    public static final String DELAY_TOPIC_NAME = "test_delay_topic";
    // 顺序消息topic
    public static final String FIFO_TOPIC_NAME = "test_fifo_topic";
}
