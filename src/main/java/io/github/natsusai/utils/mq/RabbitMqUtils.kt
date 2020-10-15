package io.github.natsusai.utils.mq

import com.rabbitmq.client.Channel
import io.github.natsusai.utils.mq.RabbitMqUtils
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Message
import org.springframework.amqp.rabbit.core.RabbitTemplate
import java.util.*

/**
 * RabbitMQ工具
 *
 * 可使用静态方法，也可创建一个实例进行调用
 * @author Kurenai
 * @since 2020-09-29 17:41
 */
class RabbitMqUtils {
    private val retryTimes: Int
    private val template: RabbitTemplate
    private val channel: Channel
    private val message: Message

    /**
     * @param retryTimes 重试次数
     * @param template RabbitTemplate
     * @param channel Channel
     * @param message Message
     */
    constructor(retryTimes: Int, template: RabbitTemplate, channel: Channel, message: Message) {
        this.retryTimes = retryTimes
        this.template = template
        this.channel = channel
        this.message = message
    }

    /**
     * @param template RabbitTemplate
     * @param channel Channel
     * @param message Message
     */
    constructor(template: RabbitTemplate, channel: Channel, message: Message) {
        retryTimes = RETRY_TIMES
        this.template = template
        this.channel = channel
        this.message = message
    }

    /**
     * 获取X-Death 计数
     * @return X-Death 计数
     */
    val xDeathCount: Long
        get() = getXDeathCount(message)

    /**
     * 获取DeliveryTag
     * @return DeliveryTag
     */
    val deliveryTag: Long
        get() = getDeliveryTag(message)

    /**
     * 获取ReDeliveryTag
     * @return ReDeliveryTag
     */
    val reDeliveryTag: Boolean
        get() = getReDeliveryTag(message)
    /**
     * 简单重试
     *
     *
     * 不使用死信队列进行重试，仅重试一次
     * @param ack 是否执行ack
     */
    /**
     * 简单重试
     *
     *
     * 不使用死信队列进行重试，仅重试一次，并做ack
     */
    @JvmOverloads
    @Throws(Exception::class)
    fun simpleRetry(ack: Boolean = true) {
        simpleRetry(channel, message, ack)
    }

    /**
     * 简单重试，否则执行传入任务
     *
     *
     * 不使用死信队列进行重试，仅重试一次；若重试失败则执行传入的任务，并执行ack
     * @param task 被执行任务
     */
    @Throws(Exception::class)
    fun simpleRetryOrExec(task: Task) {
        simpleRetryOrExec(true, task)
    }

    /**
     * 简单重试，否则执行传入任务
     *
     *
     * 不使用死信队列进行重试，仅重试一次；若重试失败则执行传入的任务
     * @param ack 是否执行ack
     * @param task 执行任务
     */
    @Throws(Exception::class)
    fun simpleRetryOrExec(ack: Boolean, task: Task) {
        simpleRetryOrExec(channel, message, ack, task)
    }
    /**
     * 重试
     * @param dlx 死信队列名称
     * @param routingKey 路由键值
     * @param ack 是否执行ack
     */
    /**
     * 重试
     * @param dlx 死信队列名称
     * @param routingKey 路由键值
     */
    /**
     * 重试
     * @param dlx 死信队列名称
     */
    @JvmOverloads
    @Throws(Exception::class)
    fun retry(dlx: String, routingKey: String = receivedRoutingKey, ack: Boolean = true) {
        doRetry(template, channel, message, dlx, routingKey, retryTimes, ack) {}
    }

    /**
     * 重试，否则执行传入任务
     * @param dlx 死信队列名称
     * @param task 被执行任务
     */
    @Throws(Exception::class)
    fun retryOrExec(dlx: String, task: Task) {
        retryOrExec(dlx, receivedRoutingKey, task)
    }

    /**
     * 重试，否则执行传入任务
     * @param dlx 死信队列名称
     * @param routingKey 路由键值
     * @param task 被执行任务
     */
    @Throws(Exception::class)
    fun retryOrExec(dlx: String, routingKey: String, task: Task) {
        retryOrExec(dlx, routingKey, true, task)
    }

    /**
     * 重试，否则执行传入任务
     * @param dlx 死信队列名称
     * @param routingKey 路由键值
     * @param ack 是否执行ack
     * @param task 被执行任务
     */
    @Throws(Exception::class)
    fun retryOrExec(dlx: String, routingKey: String, ack: Boolean, task: Task) {
        doRetry(template, channel, message, dlx, routingKey, RETRY_TIMES, ack, task)
    }

    private val receivedRoutingKey: String
        private get() = message.messageProperties.receivedRoutingKey

    fun interface Task {
        @Throws(Exception::class)
        fun execute()
    }

    companion object {
        private val log = LoggerFactory.getLogger(RabbitMqUtils::class.java)
        private const val RETRY_TIMES = 3

        /**
         * 获取X-Death 计数
         *
         * @param message Message
         * @return X-Death 计数
         */
        fun getXDeathCount(message: Message): Long {
            return Optional.ofNullable(message.messageProperties.xDeathHeader)
                .flatMap { x: List<Map<String?, *>> -> x.stream().findFirst() }
                .map { d: Map<String?, *> -> d["count"] as Long }
                .orElse(0L)
        }

        /**
         * 获取DeliveryTag
         * @return DeliveryTag
         */
        fun getDeliveryTag(message: Message): Long {
            return message.messageProperties.deliveryTag
        }

        /**
         * 获取ReDeliveryTag
         * @return ReDeliveryTag
         */
        fun getReDeliveryTag(message: Message): Boolean {
            return Optional.ofNullable(message.messageProperties.redelivered).orElse(false)
        }
        /**
         * 简单重试
         *
         *
         * 不使用死信队列进行重试，仅重试一次
         * @param channel Channel
         * @param message Message
         * @param ack 是否执行ack
         */
        /**
         * 简单重试
         *
         *
         * 不使用死信队列进行重试，仅重试一次，失败会执行ack
         * @param channel Channel
         * @param message Message
         */
        @JvmOverloads
        @Throws(Exception::class)
        fun simpleRetry(channel: Channel, message: Message, ack: Boolean = true) {
            doSimpleRetry(channel, message, ack) {}
        }

        /**
         * 简单重试，否则执行传入任务
         *
         *
         * 不使用死信队列进行重试，仅重试一次；若重试失败则执行传入的任务，并执行ack
         * @param channel Channel
         * @param message Message
         * @param task 执行任务
         */
        @Throws(Exception::class)
        fun simpleRetryOrExec(channel: Channel, message: Message, task: Task) {
            doSimpleRetry(channel, message, true, task)
        }

        /**
         * 简单重试，否则执行传入任务
         *
         *
         * 不使用死信队列进行重试，仅重试一次；若重试失败则执行传入的任务
         * @param channel Channel
         * @param message Message
         * @param ack 是否执行ack
         * @param task 执行任务
         */
        @Throws(Exception::class)
        fun simpleRetryOrExec(channel: Channel, message: Message, ack: Boolean, task: Task) {
            doSimpleRetry(channel, message, ack, task)
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
        /**
         * 重试，失败会执行ack
         * @param template RabbitTemplate
         * @param channel Channel
         * @param message Message
         * @param dlx 死信队列名称
         * @param routingKey 路由键值
         */
        /**
         * 重试，失败会执行ack
         * @param template RabbitTemplate
         * @param channel Channel
         * @param message Message
         * @param dlx 死信队列名称
         */
        @JvmOverloads
        @Throws(Exception::class)
        fun retry(
            template: RabbitTemplate,
            channel: Channel,
            message: Message,
            dlx: String,
            routingKey: String = message.messageProperties.receivedRoutingKey,
            ack: Boolean = true
        ) {
            doRetry(template, channel, message, dlx, routingKey, RETRY_TIMES, ack) {}
        }

        /**
         * 重试，否则执行传入任务，并执行ack
         * @param template RabbitTemplate
         * @param channel Channel
         * @param message Message
         * @param dlx 死信队列名称
         * @param task 被执行任务
         */
        @Throws(Exception::class)
        fun retryOrExec(
            template: RabbitTemplate, channel: Channel, message: Message, dlx: String, task: Task
        ) {
            retryOrExec(template, channel, message, dlx, message.messageProperties.receivedRoutingKey, task)
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
        @Throws(Exception::class)
        fun retryOrExec(
            template: RabbitTemplate, channel: Channel, message: Message, dlx: String, routingKey: String, task: Task
        ) {
            retryOrExec(template, channel, message, dlx, routingKey, true, task)
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
        @Throws(Exception::class)
        fun retryOrExec(
            template: RabbitTemplate,
            channel: Channel,
            message: Message,
            dlx: String,
            routingKey: String,
            ack: Boolean,
            task: Task
        ) {
            doRetry(template, channel, message, dlx, routingKey, RETRY_TIMES, ack, task)
        }

        @Throws(Exception::class)
        private fun doRetry(
            template: RabbitTemplate,
            channel: Channel,
            message: Message,
            dlx: String,
            routingKey: String,
            retryTimes: Int,
            ack: Boolean,
            task: Task
        ) {
            val xDeathCount = getXDeathCount(message)
            val canRetry = xDeathCount < retryTimes
            log.debug("retry times: {}", xDeathCount)
            if (canRetry) {
                template.convertAndSend(dlx, routingKey, message)
            } else {
                task.execute()
            }
            if (ack) {
                channel.basicAck(getDeliveryTag(message), false)
            }
        }

        @Throws(Exception::class)
        private fun doSimpleRetry(channel: Channel, message: Message, ack: Boolean, task: Task) {
            val deliveryTag = getDeliveryTag(message)
            if (getReDeliveryTag(message)) {
                task.execute()
                if (ack) {
                    channel.basicAck(deliveryTag, false)
                }
            } else {
                channel.basicReject(deliveryTag, true)
            }
        }
    }
}