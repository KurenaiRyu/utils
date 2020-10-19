package io.github.natsusai.utils.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 异步工具类
 *
 * @author Kurenai
 * @since 2020-09-18 10:10
 */

public class AsyncUtils {

  /**
   * 创建一个新线程执行传入的方法并在指定时间内获取返回值
   *
   * @param timeout  超时时间
   * @param timeUnit 时间单位
   * @param task     被调用的方法
   * @param <R>      返回类型
   * @return 返回被调用方法的返回值
   * @throws Exception 执行被调用方法异常
   */
  @SuppressWarnings("unchecked")
  public static <R> R submit(long timeout, TimeUnit timeUnit, Callable<R> task)
      throws Exception {
    FutureTask<Object> future = new FutureTask<>(() -> doTask(task));
    new Thread(future).start();
    return (R) getResult(future, timeout, timeUnit);
  }


  /**
   * 调用线程池执行传入的方法并在指定时间内获取返回值
   *
   * @param timeout  超时时间
   * @param timeUnit 时间单位
   * @param executor 线程池对象
   * @param task     被调用的方法
   * @param <R>      返回类型
   * @return 返回被调用方法的返回值
   * @throws Exception 执行被调用方法异常
   */
  @SuppressWarnings("unchecked")
  public static <R> R submit(long timeout, TimeUnit timeUnit, ExecutorService executor, Callable<R> task)
      throws Exception {
    assert executor != null;
    Future<Object> future = executor.submit(() -> doTask(task));
    return (R) getResult(future, timeout, timeUnit);
  }

  /**
   * 创建一个新线程执行传入的方法并在指定时间等待执行完成
   *
   * @param timeout  超时时间
   * @param timeUnit 时间单位
   * @param task     被调用的方法
   * @throws Exception 执行被调用方法异常
   */
  public static void execute(long timeout, TimeUnit timeUnit, Task task)
      throws Exception {
    FutureTask<Object> future = new FutureTask(() -> doTask(task));
    new Thread(future).start();
    getResult(future, timeout, timeUnit);
  }

  /**
   * 调用线程池执行传入的方法并在指定时间等待执行完成
   *
   * @param timeout  超时时间
   * @param timeUnit 时间单位
   * @param executor 线程池对象
   * @param task     被调用的方法
   * @throws Exception 执行被调用方法异常
   */
  public static void execute(long timeout, TimeUnit timeUnit, ExecutorService executor, Task task)
      throws Exception {
    assert executor != null;
    Future<Object> future = executor.submit(() -> doTask(task));
    getResult(future, timeout, timeUnit);
  }

  /**
   * 获取结果
   *
   * @param future   Future
   * @param timeout  超时时间
   * @param timeUnit 超时时间单位
   * @return 返回执行结果
   * @throws Exception 执行任务当中出现异常
   */
  private static <R> R getResult(Future<R> future, long timeout, TimeUnit timeUnit)
      throws Exception {
    R result;
    try {
      result = future.get(timeout, timeUnit);
    } catch (TimeoutException ignored) {
      return null;
    }
    return result;
  }

  /**
   * 执行任务
   *
   * @param task 任务
   * @return 异常或是执行结果对象
   */
  private static Object doTask(Callable<?> task) throws Exception {
    return task.call();
  }

  /**
   * 执行任务
   *
   * @param task 任务
   * @return 异常对象
   */
  private static Object doTask(Task task) throws Exception {
    task.run();
    return null;
  }

  @FunctionalInterface
  public interface Task {

    void run() throws Exception;
  }

}
