package batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import java.util.ArrayList;
import java.util.List;

public class Producer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");

        producer.setNamesrvAddr("159.75.115.106:9876");

        producer.start();

        String topic = "BatchTest";
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "BatchTopic", "OrderID001", "Hello world 0".getBytes()));
        messages.add(new Message(topic, "BatchTopic", "OrderID002", "Hello world 1".getBytes()));
        messages.add(new Message(topic, "BatchTopic", "OrderID003", "Hello world 2".getBytes()));
        try {
            producer.send(messages);
        } catch (Exception e) {
            e.printStackTrace();
            //处理error
        }

        SendResult sendResult = producer.send(messages);

        System.out.println("发送结果："+sendResult);

        producer.shutdown();

    }

}
