package mao.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Project name(项目名称)：RocketMQ_延迟消息的发送与接收
 * Package(包名): mao.producer
 * Class(类名): DelayProducer
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/5
 * Time(创建时间)： 20:52
 * Version(版本): 1.0
 * Description(描述)： 生产者-延迟消息
 */

public class DelayProducer
{
    /**
     * 简单日期格式
     */
    static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");

    public static void main(String[] args)
            throws MQClientException, MQBrokerException, RemotingException, InterruptedException
    {
        //生产者
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("mao_group");
        //设置nameserver地址
        defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
        //启动
        defaultMQProducer.start();
        //发送100条延迟消息
        for (int i = 0; i < 100; i++)
        {
            //时间
            String format = simpleDateFormat.format(new Date());
            //消息对象
            Message message = new Message("test_topic", (format + " --> 消息" + i).getBytes(StandardCharsets.UTF_8));
            //设置延时，水平4为30秒
            message.setDelayTimeLevel(4);
            //发送消息
            defaultMQProducer.send(message);
        }
        //关闭
        defaultMQProducer.shutdown();
    }
}
