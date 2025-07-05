package test.icetea.rocketmq.client.normal;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import test.icetea.rocketmq.client.ServerConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PullConsumerNormalExample {


    private static final String pullConsumerGroupName = "test_topic_pull_consumer_group";

    public static void main(String[] args) throws ClientException, IOException, InterruptedException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
        String endpoints = ServerConfig.ENDPOINTS;
        String topicName = ServerConfig.NORMAL_TOPIC_NAME;

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
        SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(pullConsumerGroupName) // 为消费者指定所属的消费者分组，Group需要提前创建。
                .setSubscriptionExpressions(subscriptionExpressionMap)// 设置预绑定的订阅关系。
                .setAwaitDuration(Duration.ofSeconds(10)) // duration for long-polling. 设置拉取消息阻塞时间：如果当前没有可用消息，消费者会阻塞等待，期间如果有新消息到达，就立即返回；否则，超时后返回 null。
                .build();


        // Max message num for each long polling.
        int maxMessageNum = 16;
        // Set message invisible duration after it is received.
        // 当消费者拉取了一条消息之后，这条消息在一段时间内对其他消费者（或同一个消费者未确认的情况下）是“不可见”的，防止被重复消费。
        // 设置小了会报错
        Duration invisibleDuration = Duration.ofSeconds(15);

        // 同步拉取
        List<MessageView> messageViewList = simpleConsumer.receive(maxMessageNum, invisibleDuration);
        messageViewList.forEach(m -> {
            System.out.printf("[%s] consume message successfully, messageId=%s, mb=%s \n", Thread.currentThread().getName(), m.getMessageId(), StandardCharsets.UTF_8.decode(m.getBody()).toString());
            try {
                // ack是必须的，否则消息会被重新投递
                simpleConsumer.ack(m);
                System.out.printf("ack success, messageId={} \n", m.getMessageId());
            } catch (ClientException e) {
                System.out.printf("ack error, messageId={} \n", m.getMessageId());
                e.printStackTrace();
            }
        });

        // 异步拉取
        CompletableFuture<List<MessageView>> completableFuture = simpleConsumer.receiveAsync(maxMessageNum, invisibleDuration);
        completableFuture.whenCompleteAsync((mvs, e) -> {
            mvs.forEach(m -> {
                System.out.printf("[%s] consume message successfully, messageId=%s, mb=%s \n", Thread.currentThread().getName(), m.getMessageId(), StandardCharsets.UTF_8.decode(m.getBody()).toString());
                try {
                    simpleConsumer.ack(m);
                    System.out.printf("ack success, messageId={} \n", m.getMessageId());
                } catch (ClientException ex) {
                    System.out.printf("ack error, messageId={} \n", m.getMessageId());
                    e.printStackTrace();
                }
            });
        });


        // 如果不需要再使用 PushConsumer，可关闭该实例。
//        pushConsumer.close();
    }

}