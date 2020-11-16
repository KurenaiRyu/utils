package io.github.natsusai.utils.concurrent

/**
 * 回调任务接口
 */
interface CallbackTask<T> {
    @Throws(Exception::class)
    fun callback(result: T)
}