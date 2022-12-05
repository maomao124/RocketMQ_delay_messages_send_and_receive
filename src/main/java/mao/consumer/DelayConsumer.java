package mao.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Project name(项目名称)：RocketMQ_延迟消息的发送与接收
 * Package(包名): mao.consumer
 * Class(类名): DelayConsumer
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/5
 * Time(创建时间)： 21:00
 * Version(版本): 1.0
 * Description(描述)： 消费者-延时消息
 */

public class DelayConsumer
{
    /**
     * 简单日期格式
     */
    static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");

    public static void main(String[] args) throws MQClientException
    {
        //消费者
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("mao_group");
        //订阅
        defaultMQPushConsumer.subscribe("test_topic", "*");
        //注册监听器
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently()
        {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext)
            {
                //当前时间
                String format = simpleDateFormat.format(new Date());
                for (MessageExt messageExt : list)
                {
                    //打印消息
                    System.out.println("当前时间：" + format + "    " + "队列id：" + messageExt.getQueueId()
                            + "    消息：" + new String(messageExt.getBody(), StandardCharsets.UTF_8));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动
        defaultMQPushConsumer.start();
    }
}
