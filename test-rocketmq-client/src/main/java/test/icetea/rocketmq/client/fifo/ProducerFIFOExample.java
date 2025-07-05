package test.icetea.rocketmq.client.fifo;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;

import java.io.IOException;

import static test.icetea.rocketmq.client.ServerConfig.ENDPOINTS;
import static test.icetea.rocketmq.client.ServerConfig.FIFO_TOPIC_NAME;

/**
 * 顺序消息
 */
public class ProducerFIFOExample {

    public static void main(String[] args) throws ClientException, IOException, InterruptedException {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
                .setEndpoints(ENDPOINTS)
                .build();

        // 初始化Producer时需要设置通信配置以及预绑定的Topic。
        Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(configuration)
                .setTopics(FIFO_TOPIC_NAME)
                .setMaxAttempts(3) // 重试次数， 默认3
                .build();

        // 消息发送
        MessageBuilder messageBuilder = new MessageBuilderImpl();
        Message message = messageBuilder.setTopic(FIFO_TOPIC_NAME)
                //设置顺序消息的排序分组，该分组尽量保持离散，避免热点排序分组。
                .setMessageGroup("fifoGroup001")
                .setKeys("messageKey") //设置消息索引键，可根据关键字精确查找某条消息。
                .setBody("icetea test fifo message".getBytes()) // 消息体
                .build();

        int count = 0;
        while (count < 100) {
            try {
                //发送消息，需要关注发送结果，并捕获失败等异常。
                SendReceipt sendReceipt = producer.send(message);
                System.out.printf("Send fifo message successfully, messageId=%s \n", sendReceipt.getMessageId());
            } catch (ClientException e) {
                System.out.println("send fifo message error");
                e.printStackTrace();
            }
            count++;
        }

        producer.close();
    }
}