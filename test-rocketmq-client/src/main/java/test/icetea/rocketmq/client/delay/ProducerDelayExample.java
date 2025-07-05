package test.icetea.rocketmq.client.delay;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;

import java.io.IOException;

import static test.icetea.rocketmq.client.ServerConfig.DELAY_TOPIC_NAME;
import static test.icetea.rocketmq.client.ServerConfig.ENDPOINTS;

/**
 * 延迟消息
 */
public class ProducerDelayExample {

    public static void main(String[] args) throws ClientException, IOException, InterruptedException {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
                .setEndpoints(ENDPOINTS)
                .build();

        // 初始化Producer时需要设置通信配置以及预绑定的Topic。
        Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(configuration)
                .setTopics(DELAY_TOPIC_NAME)
                .setMaxAttempts(3) // 重试次数， 默认3
                .build();

        // 定时/延时消息发送
        MessageBuilder messageBuilder = new MessageBuilderImpl();
        // 延迟时间为6s之后的Unix时间戳。
        Long deliverTimeStamp = System.currentTimeMillis() + 6000L;
        Message message = messageBuilder.setTopic(DELAY_TOPIC_NAME)
                .setKeys("messageKey") //设置消息索引键，可根据关键字精确查找某条消息。
                .setDeliveryTimestamp(deliverTimeStamp)  // 用于延迟消息定时投递
                .setBody("icetea test delay message".getBytes()) // 消息体
                .build();

        try {
            //发送消息，需要关注发送结果，并捕获失败等异常。
            SendReceipt sendReceipt = producer.send(message);
            System.out.printf("Send delay message successfully, messageId=%s \n", sendReceipt.getMessageId());
        } catch (ClientException e) {
            System.out.println("send delay message error");
            e.printStackTrace();
        }

        producer.close();
    }
}