package test.icetea.rocketmq.client;

import org.apache.rocketmq.client.apis.*;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class PushConsumerExample {

    public static void main(String[] args) throws ClientException, IOException, InterruptedException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
        String endpoints = "127.0.0.1:8081";
        String accessKey = "icetea";
        String secretKey = "icetea";
        SessionCredentialsProvider sessionCredentialsProvider =
                new StaticSessionCredentialsProvider(accessKey, secretKey);

        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setCredentialProvider(sessionCredentialsProvider)
                .setEndpoints(endpoints)
                .build();
        // 订阅消息的过滤规则，表示订阅所有Tag的消息。
        String tag = "*";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        // 为消费者指定所属的消费者分组，Group需要提前创建。
        String consumerGroup = "test_consumer_group";
        // 指定需要订阅哪个目标Topic，Topic需要提前创建。
        String topic = "test_topic";
        String delay_topic = "test_delay_topic";
        Map<String, FilterExpression> map = new HashMap<>();
        map.put(topic, filterExpression);
        map.put(delay_topic, filterExpression);

        // 初始化PushConsumer，需要绑定消费者分组ConsumerGroup、通信参数以及订阅关系。
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                // 设置消费者分组。
                .setConsumerGroup(consumerGroup)
                // 设置预绑定的订阅关系。
                .setSubscriptionExpressions(map)
                .setConsumptionThreadCount(1)
                .setMaxCacheMessageCount(1)
                // 设置消费监听器。
                .setMessageListener(messageView -> {
                    // 处理消息并返回消费结果。
                    Charset charset = StandardCharsets.UTF_8;
                    String mb = charset.decode(messageView.getBody()).toString();
                    System.out.printf("Consume message successfully, messageId=%s, mb=%s", messageView.getMessageId(), mb);
                    return ConsumeResult.SUCCESS;
                }).build();

        // 如果不需要再使用 PushConsumer，可关闭该实例。
//        pushConsumer.close();
    }

}