package transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("group5");

        producer.setNamesrvAddr("159.75.115.106:9876");
        producer.setTransactionListener(new TransactionListener(){
            /**
             * 在该方法中执行本地事务
             * @param msg
             * @param arg
             * @return
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg,Object arg){
                if(StringUtils.equals("TAGA",msg.getTags())){
                    return LocalTransactionState.COMMIT_MESSAGE;
                }else if(StringUtils.equals("TAGB",msg.getTags())){
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }else if(StringUtils.equals("TAGC",msg.getTags())){
                    return LocalTransactionState.UNKNOW;
                }
                return LocalTransactionState.UNKNOW;
            }

            /**
             * 该方法是MQ进行事务状态回查
             * @param messageExt
             * @return
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                //TAG会打印
                System.out.println("消息的Tag:"+messageExt.getTags());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        producer.start();

        String[] tag = {"TAGA","TAGB","TAGC"};

        for (int i = 0; i < 3; i++) {

            Message msg = new Message("TransactionTopic", tag[i], ("hello world"+i).getBytes());

            SendResult sendResult = producer.sendMessageInTransaction(msg,null);

            SendStatus status = sendResult.getSendStatus();

            System.out.println("发送结果："+status);

            TimeUnit.SECONDS.sleep(1);
        }

        //因为存在消息回传，所以发送完消息后不建议立即停掉
        //producer.shutdown();

    }

}
