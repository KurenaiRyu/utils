package io.github.natsusai.utils.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: 根据特定的id或者内容进行锁定

/**
 * 锁工具类
 *
 * @author Kurenai
 * @since 2020-06-28 11:34
 */

public class Locker {

  private static final int      MAX_COUNT = 3;
  private static final long     TIME_OUT  = 500;
  private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

  private static final Logger log = LoggerFactory.getLogger(Locker.class);

  /**
   * 尝试获取锁并执行传入方法
   *
   * @param lock     锁对象
   * @param executor 被执行的方法
   * @param <T>      返回值类型
   * @return 返回被执行方法所返回的结果
   * @throws Exception            执行方法异常
   * @throws InterruptedException 获取锁失败
   * @throws LockerException      获取锁超过重复次数上限
   */
  public static <T> T tryLock(Lock lock, Executor<T> executor) throws Exception {
    return tryLock(lock, TIME_OUT, TIME_UNIT, executor);
  }

  /**
   * 尝试获取锁并执行传入方法
   *
   * @param lock     锁对象
   * @param timeOut  每次获取锁超时时间
   * @param timeUnit 时间单位
   * @param executor 被执行的方法
   * @param <T>      返回值类型
   * @return 返回被执行方法所返回的结果
   * @throws Exception            执行方法异常
   * @throws InterruptedException 获取锁失败
   * @throws LockerException      获取锁超过重复次数上限
   */
  public static <T> T tryLock(Lock lock, long timeOut, TimeUnit timeUnit, Executor<T> executor)
      throws Exception {
    boolean locked = false;
    T       result;
    try {
      locked = tryLock(lock, timeOut, timeUnit);
      result = executor.execute();
    } finally {
      if (locked) {
        lock.unlock();
      }
    }
    return result;
  }

  /**
   * 直接获取锁并执行传入方法
   *
   * @param lock     锁对象
   * @param executor 被执行的方法
   * @param <T>      返回值类型
   * @return 返回被执行方法所返回的结果
   * @throws Exception            执行方法异常
   * @throws InterruptedException 获取锁失败
   */
  public static <T> T lock(Lock lock, Executor<T> executor) throws Exception {
    T result;
    try {
      lock.lockInterruptibly();
      result = executor.execute();
    } finally {
      try {
        lock.unlock();
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    }
    return result;
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
  private static boolean tryLock(Lock lock, long timeOut, TimeUnit timeUnit)
      throws InterruptedException, ExecutionException, LockerException {
    boolean                  locked   = (lock.tryLock() || lock.tryLock(timeOut, timeUnit));
    int                      count    = 1;
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    while (!locked) {
      if (count >= MAX_COUNT) {
        throw new LockerException("Retry over max times [" + MAX_COUNT + "].");
      }
      ScheduledFuture<Boolean> future = executor.schedule(() -> {
        try {
          return (lock.tryLock() || lock.tryLock(timeOut, timeUnit));
        } catch (InterruptedException e) {
          log.error(e.getMessage(), e);
        }
        return false;
      }, 200, TimeUnit.MILLISECONDS);
      locked = future.get();
      count++;
    }
    return true;
  }

  /**
   * Executor
   *
   * @param <T> 返回值类型
   */
  @FunctionalInterface
  public interface Executor<T> {

    T execute() throws Exception;
  }
}
