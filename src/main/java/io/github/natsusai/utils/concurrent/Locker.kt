package io.github.natsusai.utils.concurrent

import org.slf4j.LoggerFactory
import java.util.concurrent.*
import java.util.concurrent.locks.Lock

//TODO: 根据特定的id或者内容进行锁定
/**
 * 锁工具类
 *
 * @author Kurenai
 * @since 2020-06-28 11:34
 */
class Locker {

    companion object{
        private const val MAX_COUNT = 3
        private const val TIME_OUT: Long = 500
        private val TIME_UNIT = TimeUnit.MILLISECONDS
    }
    private val log = LoggerFactory.getLogger(Locker::class.java)

    /**
     * 尝试获取锁并执行传入方法
     *
     * @param lock     锁对象
     * @param task 被执行的方法
     * @param <T>      返回值类型
     * @return 返回被执行方法所返回的结果
     * @throws Exception            执行方法异常
     * @throws InterruptedException 获取锁失败
     * @throws LockerException      获取锁超过重复次数上限
    </T> */
    @Throws(Exception::class)
    fun <T> tryLock(lock: Lock, task: Callable<T>): T {
        return tryLock(lock, TIME_OUT, TIME_UNIT, task)
    }

    /**
     * 尝试获取锁并执行传入方法
     *
     * @param lock     锁对象
     * @param timeOut  每次获取锁超时时间
     * @param timeUnit 时间单位
     * @param task 被执行的方法
     * @param <T>      返回值类型
     * @return 返回被执行方法所返回的结果
     * @throws Exception            执行方法异常
     * @throws InterruptedException 获取锁失败
     * @throws LockerException      获取锁超过重复次数上限
    </T> */
    @Throws(Exception::class)
    fun <T> tryLock(lock: Lock, timeOut: Long, timeUnit: TimeUnit, task: Callable<T>): T {
        var locked = false
        val result: T
        try {
            locked = tryLock(lock, timeOut, timeUnit)
            result = task.call()
        } finally {
            if (locked) {
                lock.unlock()
            }
        }
        return result
    }

    /**
     * 直接获取锁并执行传入方法
     *
     * @param lock     锁对象
     * @param task 被执行的方法
     * @param <T>      返回值类型
     * @return 返回被执行方法所返回的结果
     * @throws Exception            执行方法异常
     * @throws InterruptedException 获取锁失败
    </T> */
    @Throws(Exception::class)
    fun <T> lock(lock: Lock, task: Callable<T>): T {
        return try {
            lock.lockInterruptibly()
            task.call()
        } finally {
            lock.unlock()
        }
    }

    /**
     * 直接获取锁并执行传入方法
     *
     * @param lock     锁对象
     * @param task 被执行的方法
     * @param callbackTask 回调方法
     * @param <T>      返回值类型
     * @return 返回被执行方法所返回的结果
     * @throws Exception            执行方法异常
     * @throws InterruptedException 获取锁失败
    </T> */
    @Throws(Exception::class)
    fun <T> lock(lock: Lock, task: Callable<T>, callbackTask: CallbackTask<T>) {
        try {
            lock.lockInterruptibly()
            val future= FutureTask(task)
            Thread(future).start()
            Thread { callbackTask.callback(future.get()) }
        } finally {
            lock.unlock()
        }
    }

    /**
     * 直接获取锁并执行传入方法
     *
     * @param lock     锁对象
     * @param task 被执行的方法
     * @param callbackTask 回调方法
     * @param executor 线程池
     * @param <T>      返回值类型
     * @return 返回被执行方法所返回的结果
     * @throws Exception            执行方法异常
     * @throws InterruptedException 获取锁失败
    </T> */
    @Throws(Exception::class)
    fun <T> lock(lock: Lock, task: Callable<T>, callbackTask: CallbackTask<T>, executor: ExecutorService) {
        try {
            lock.lockInterruptibly()
            val future= FutureTask(task)
            executor.execute(future)
            executor.execute {
                callbackTask.callback(future.get())
            }
        } finally {
            lock.unlock()
        }
    }

    /**
     * 直接获取锁并执行传入方法
     *
     * @param lock     锁对象
     * @param task 被执行的方法
     * @param callbackTask 回调方法
     * @param executor 线程池
     * @param <T>      返回值类型
     * @return 返回被执行方法所返回的结果
     * @throws Exception            执行方法异常
     * @throws InterruptedException 获取锁失败
    </T> */
    @Throws(Exception::class)
    fun <T> lock(lock: Lock, task: Callable<T>, callbackTask: CallbackTask<T>, caseHandle: CallbackTask<Throwable>, executor: ExecutorService) {
        try {
            lock.lockInterruptibly()
            val future = FutureTask(task)
            executor.execute(future)
            executor.execute {
                callbackTask.callback(future.get())
            }
        } catch (e: Exception) {
            caseHandle.callback(e)
        } finally {
            lock.unlock()
        }
    }

    /**
     * 尝试获取锁直到重复次数上限
     *
     * @param lock     锁对象
     * @param timeOut  超时时间
     * @param timeUnit 时间单位
     * @return 成功则返回true
     * @throws InterruptedException 获取锁失败
     * @throws LockerException      获取锁超过重复次数上限
     */
    @Throws(InterruptedException::class, ExecutionException::class, LockerException::class)
    private fun tryLock(lock: Lock, timeOut: Long, timeUnit: TimeUnit): Boolean {
        var locked = lock.tryLock(timeOut, timeUnit)
        var count = 1
        val executor = Executors.newSingleThreadScheduledExecutor()
        while (!locked) {
            if (count >= MAX_COUNT) {
                throw LockerException("Retry over max times [$MAX_COUNT].")
            }
            val future = executor.schedule<Boolean>({
                try {
                    return@schedule lock.tryLock(timeOut, timeUnit)
                } catch (e: InterruptedException) {
                    log.error(e.message, e)
                }
                false
            }, 200, TimeUnit.MILLISECONDS)
            locked = future.get()
            count++
        }
        return true
    }
}