package test.icetea.rocketmq.client.fifo;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import test.icetea.rocketmq.client.ServerConfig;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * 消费 顺序消息
 */
public class ConsumerFIFOExample {

    private static final String fifoConsumerGroup = "test_fifo_topic_consumer_group";

    public static void main(String[] args) throws ClientException, IOException, InterruptedException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
        String endpoints = ServerConfig.ENDPOINTS;
        String topicName = ServerConfig.FIFO_TOPIC_NAME;

        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .enableSsl(false)
                .setRequestTimeout(Duration.ofSeconds(3)) // 请求超时时间，默认3s
                .setEndpoints(endpoints)
                .build();
        // 订阅消息的过滤规则，表示订阅所有Tag的消息。
        String tag = "*";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        Map<String, FilterExpression> subscriptionExpressionMap = new HashMap<>();
        subscriptionExpressionMap.put(topicName, filterExpression);

        // 初始化PushConsumer，需要绑定消费者分组ConsumerGroup、通信参数以及订阅关系。
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(fifoConsumerGroup) // 设置消费者分组，需在rocketmq中预先配置
                .setSubscriptionExpressions(subscriptionExpressionMap)// 设置预绑定的订阅关系。
                .setConsumptionThreadCount(1) // 消费者线程数 要保障消费者顺序消费的话，需要设置为1
                .setMaxCacheMessageCount(10) // 本地缓存消息数量 默认1024，
                .setMaxCacheMessageSizeInBytes(67108864) // 缓存消息的最大byte 默认 67108864 byte = 67.108864 mb
                // 设置消费监听器。
                .setMessageListener(messageView -> {
                    // 处理消息并返回消费结果。
                    Charset charset = StandardCharsets.UTF_8;
                    String mb = charset.decode(messageView.getBody()).toString();
                    System.out.printf("[%s] consume fifo message successfully, messageId=%s, content=%s \n", Thread.currentThread().getName(), messageView.getMessageId(), mb);
                    return ConsumeResult.SUCCESS;
                }).build();

        // 如果不需要再使用 PushConsumer，可关闭该实例。
//        pushConsumer.close();
    }

}