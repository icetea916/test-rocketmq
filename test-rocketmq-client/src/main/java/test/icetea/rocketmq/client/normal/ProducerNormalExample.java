package test.icetea.rocketmq.client.normal;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import test.icetea.rocketmq.client.ServerConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 普通消息：生产者示例
 */
public class ProducerNormalExample {

    public static void main(String[] args) throws ClientException, IOException, InterruptedException {
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
        String endpoint = ServerConfig.ENDPOINTS;
        // 消息发送的目标Topic名称，需要提前创建。
        String topic = ServerConfig.NORMAL_TOPIC_NAME;
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
                .setRequestTimeout(Duration.ofSeconds(5))
                .setEndpoints(endpoint)
                .build();
        // 初始化Producer时需要设置通信配置以及预绑定的Topic。
        Producer producer = provider.newProducerBuilder()
                .setTopics(topic)
                .setClientConfiguration(configuration)
                .build();

        // 普通消息发送。
        Message message = provider.newMessageBuilder()
                .setTopic(topic)
                // 设置消息索引键，可根据关键字精确查找某条消息。
                .setKeys("messageKey")
                // 设置消息Tag，用于消费端根据指定Tag过滤消息。
//                .setTag("messageTag")
                // 消息体。
                .setBody("icetea test message".getBytes())
                .build();

        // 同步发送
        try {
            // 发送消息，需要关注发送结果，并捕获失败等异常。
            SendReceipt sendReceipt = producer.send(message);
            System.out.printf("Send message successfully, messageId=%s \n", sendReceipt.getMessageId());
        } catch (ClientException e) {
            System.out.printf("Failed to send message, %s", e);
        }


        // 异步发送
        int i = 0;
        while (i < 1000000) {
            CompletableFuture<SendReceipt> sendReceiptCompletableFuture = producer.sendAsync(message);
            sendReceiptCompletableFuture.whenComplete((r, e) -> {
                if (e != null) {
                    e.printStackTrace();
                    return;
                }
                System.out.printf("Send message successfully, messageId=%s \n", r.getMessageId());
            });
            i++;
        }

        System.out.println("完成");

        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);

        producer.close();
    }
}