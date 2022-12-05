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
 * Class(类名): DelayProducer2
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/5
 * Time(创建时间)： 21:11
 * Version(版本): 1.0
 * Description(描述)： 无
 */

public class DelayProducer2
{
    /**
     * 简单日期格式
     */
    static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");

    /**
     * 得到int随机
     *
     * @param min 最小值
     * @param max 最大值
     * @return int
     */
    public static int getIntRandom(int min, int max)
    {
        if (min > max)
        {
            min = max;
        }
        return min + (int) (Math.random() * (max - min + 1));
    }

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
            //设置随机延时
            //int level = getIntRandom(1, 4);
            int level = i < 50 ? 4 : 2;
            message.setDelayTimeLevel(level);
            //发送消息
            defaultMQProducer.send(message);
            //打印
            System.out.println(format + " --> 消息" + i + "    延迟水平：" + level);
        }
        //关闭
        defaultMQProducer.shutdown();
    }
}
