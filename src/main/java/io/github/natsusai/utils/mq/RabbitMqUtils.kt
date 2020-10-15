package io.github.natsusai.utils.mq;

import com.rabbitmq.client.Channel;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * RabbitMQ工具
 *
 * 可使用静态方法，也可创建一个实例进行调用
 * @author Kurenai
 * @since 2020-09-29 17:41
 */

public class RabbitMqUtils {

  private static final Logger log = LoggerFactory.getLogger(RabbitMqUtils.class);

  private static final int RETRY_TIMES = 3;

  private final int            retryTimes;
  private final RabbitTemplate template;
  private final Channel        channel;
  private final Message        message;

  /**
   * @param retryTimes 重试次数
   * @param template RabbitTemplate
   * @param channel Channel
   * @param message Message
   */
  public RabbitMqUtils(int retryTimes, RabbitTemplate template, Channel channel, Message message) {
    this.retryTimes = retryTimes;
    this.template   = template;
    this.channel    = channel;
    this.message    = message;
  }

  /**
   * @param template RabbitTemplate
   * @param channel Channel
   * @param message Message
   */
  public RabbitMqUtils(RabbitTemplate template, Channel channel, Message message) {
    this.retryTimes = RETRY_TIMES;
    this.template   = template;
    this.channel    = channel;
    this.message    = message;
  }

  /**
   * 获取X-Death 计数
   * @return X-Death 计数
   */
  public long getXDeathCount() {
    return getXDeathCount(message);
  }

  /**
   * 获取DeliveryTag
   * @return DeliveryTag
   */
  public long getDeliveryTag() {
    return getDeliveryTag(message);
  }


  /**
   * 获取ReDeliveryTag
   * @return ReDeliveryTag
   */
  public boolean getReDeliveryTag() {
    return getReDeliveryTag(message);
  }

  /**
   * 简单重试
   * <p/>
   * 不使用死信队列进行重试，仅重试一次，并做ack
   */
  public void simpleRetry() throws Exception {
    simpleRetry(true);
  }

  /**
   * 简单重试
   * <p/>
   * 不使用死信队列进行重试，仅重试一次
   * @param ack 是否执行ack
   */
  public void simpleRetry(boolean ack) throws Exception {
    simpleRetry(channel, message, ack);
  }

  /**
   * 简单重试，否则执行传入任务
   * <p/>
   * 不使用死信队列进行重试，仅重试一次；若重试失败则执行传入的任务，并执行ack
   * @param task 被执行任务
   */
  public void simpleRetryOrExec(Task task) throws Exception {
    simpleRetryOrExec(true, task);
  }

  /**
   * 简单重试，否则执行传入任务
   * <p/>
   * 不使用死信队列进行重试，仅重试一次；若重试失败则执行传入的任务
   * @param ack 是否执行ack
   * @param task 执行任务
   */
  public void simpleRetryOrExec(boolean ack, Task task) throws Exception {
    simpleRetryOrExec(channel, message, ack, task);
  }

  /**
   * 重试
   * @param dlx 死信队列名称
   */
  public void retry(String dlx) throws Exception {
    retry(dlx, getReceivedRoutingKey());
  }

  /**
   * 重试
   * @param dlx 死信队列名称
   * @param routingKey 路由键值
   */
  public void retry(String dlx, String routingKey) throws Exception {
    retry(dlx, routingKey, true);
  }

  /**
   * 重试
   * @param dlx 死信队列名称
   * @param routingKey 路由键值
   * @param ack 是否执行ack
   */
  public void retry(String dlx, String routingKey, boolean ack) throws Exception {
    doRetry(template, channel, message, dlx, routingKey, retryTimes, ack, () -> {});
  }

  /**
   * 重试，否则执行传入任务
   * @param dlx 死信队列名称
   * @param task 被执行任务
   */
  public void retryOrExec(String dlx, Task task) throws Exception {
    retryOrExec(dlx, getReceivedRoutingKey(), task);
  }

  /**
   * 重试，否则执行传入任务
   * @param dlx 死信队列名称
   * @param routingKey 路由键值
   * @param task 被执行任务
   */
  public void retryOrExec(String dlx, String routingKey, Task task) throws Exception {
    retryOrExec(dlx, routingKey, true, task);
  }

  /**
   * 重试，否则执行传入任务
   * @param dlx 死信队列名称
   * @param routingKey 路由键值
   * @param ack 是否执行ack
   * @param task 被执行任务
   */
  public void retryOrExec(String dlx, String routingKey, boolean ack, Task task) throws Exception {
    doRetry(template, channel, message, dlx, routingKey, RETRY_TIMES, ack, task);
  }

  /**
   * 获取X-Death 计数
   *
   * @param message Message
   * @return X-Death 计数
   */
  public static long getXDeathCount(Message message) {
    return Optional.ofNullable(message.getMessageProperties().getXDeathHeader())
        .flatMap(x -> x.stream().findFirst())
        .map(d -> (long) d.get("count"))
        .orElse(0L);
  }

  /**
   * 获取DeliveryTag
   * @return DeliveryTag
   */
  public static long getDeliveryTag(Message message) {
    return message.getMessageProperties().getDeliveryTag();
  }

  /**
   * 获取ReDeliveryTag
   * @return ReDeliveryTag
   */
  public static boolean getReDeliveryTag(Message message) {
    return Optional.ofNullable(message.getMessageProperties().getRedelivered()).orElse(false);
  }

  /**
   * 简单重试
   * <p/>
   * 不使用死信队列进行重试，仅重试一次，失败会执行ack
   * @param channel Channel
   * @param message Message
   */
  public static void simpleRetry(Channel channel, Message message) throws Exception {
    simpleRetry(channel, message, true);
  }

  /**
   * 简单重试
   * <p/>
   * 不使用死信队列进行重试，仅重试一次
   * @param channel Channel
   * @param message Message
   * @param ack 是否执行ack
   */
  public static void simpleRetry(Channel channel, Message message, boolean ack) throws Exception {
    doSimpleRetry(channel, message, ack, () -> {});
  }

  /**
   * 简单重试，否则执行传入任务
   * <p/>
   * 不使用死信队列进行重试，仅重试一次；若重试失败则执行传入的任务，并执行ack
   * @param channel Channel
   * @param message Message
   * @param task 执行任务
   */
  public static void simpleRetryOrExec(Channel channel, Message message, Task task) throws Exception {
    doSimpleRetry(channel, message, true, task);
  }

  /**
   * 简单重试，否则执行传入任务
   * <p/>
   * 不使用死信队列进行重试，仅重试一次；若重试失败则执行传入的任务
   * @param channel Channel
   * @param message Message
   * @param ack 是否执行ack
   * @param task 执行任务
   */
  public static void simpleRetryOrExec(Channel channel, Message message, boolean ack, Task task) throws Exception {
    doSimpleRetry(channel, message, ack, task);
  }

  /**
   * 重试，失败会执行ack
   * @param template RabbitTemplate
   * @param channel Channel
   * @param message Message
   * @param dlx 死信队列名称
   */
  public static void retry(RabbitTemplate template, Channel channel, Message message, String dlx) throws Exception {
    retry(template, channel, message, dlx, message.getMessageProperties().getReceivedRoutingKey());
  }

  /**
   * 重试，失败会执行ack
   * @param template RabbitTemplate
   * @param channel Channel
   * @param message Message
   * @param dlx 死信队列名称
   * @param routingKey 路由键值
   */
  public static void retry(RabbitTemplate template, Channel channel, Message message, String dlx, String routingKey)
      throws Exception {
    retry(template, channel, message, dlx, routingKey, true);
  }

  /**
   * 重试
   * @param template RabbitTemplate
   * @param channel Channel
   * @param message Message
   * @param dlx 死信队列名称
   * @param routingKey 路由键值
   * @param ack 是否执行ack
   */
  public static void retry(RabbitTemplate template, Channel channel, Message message, String dlx, String routingKey,
      boolean ack)
      throws Exception {
    doRetry(template, channel, message, dlx, routingKey, RETRY_TIMES, ack, () -> {});
  }

  /**
   * 重试，否则执行传入任务，并执行ack
   * @param template RabbitTemplate
   * @param channel Channel
   * @param message Message
   * @param dlx 死信队列名称
   * @param task 被执行任务
   */
  public static void retryOrExec(
      RabbitTemplate template, Channel channel, Message message, String dlx, Task task) throws Exception {
    retryOrExec(template, channel, message, dlx, message.getMessageProperties().getReceivedRoutingKey(), task);
  }

  /**
   * 重试，否则执行传入任务，并执行ack
   * @param template RabbitTemplate
   * @param channel Channel
   * @param message Message
   * @param dlx 死信队列名称
   * @param routingKey 路由键值
   * @param task 被执行任务
   */
  public static void retryOrExec(
      RabbitTemplate template, Channel channel, Message message, String dlx, String routingKey, Task task)
      throws Exception {
    retryOrExec(template, channel, message, dlx, routingKey, true, task);
  }

  /**
   * 重试，否则执行传入任务
   * @param template RabbitTemplate
   * @param channel Channel
   * @param message Message
   * @param dlx 死信队列名称
   * @param routingKey 路由键值
   * @param ack 是否执行ack
   * @param task 被执行任务
   */
  public static void retryOrExec(
      RabbitTemplate template, Channel channel, Message message, String dlx, String routingKey, boolean ack, Task task)
      throws Exception {
    doRetry(template, channel, message, dlx, routingKey, RETRY_TIMES, ack, task);
  }

  private String getReceivedRoutingKey() {
    return message.getMessageProperties().getReceivedRoutingKey();
  }

  private static void doRetry(
      RabbitTemplate template, Channel channel, Message message, String dlx, String routingKey, int retryTimes,
      boolean ack, Task task) throws Exception {
    final long    xDeathCount = getXDeathCount(message);
    final boolean canRetry    = xDeathCount < retryTimes;
    log.debug("retry times: {}", xDeathCount);
    if (canRetry) {
      template.convertAndSend(dlx, routingKey, message);
    } else {
      task.execute();
    }
    if (ack) {
      channel.basicAck(getDeliveryTag(message), false);
    }
  }

  private static void doSimpleRetry(Channel channel, Message message, boolean ack, Task task)
      throws Exception {
    long deliveryTag = getDeliveryTag(message);
    if (getReDeliveryTag(message)) {
      task.execute();
      if (ack) {
        channel.basicAck(deliveryTag, false);
      }
    } else {
      channel.basicReject(deliveryTag, true);
    }
  }

  @FunctionalInterface
  public interface Task {
    void execute() throws Exception;
  }
}
