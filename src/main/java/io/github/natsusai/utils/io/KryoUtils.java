package io.github.natsusai.utils.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.commons.codec.binary.Base64;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * Kryo Utils
 * @author Kurenai
 * @since 2020-10-14
 */
public class KryoUtils {

  private static final String   DEFAULT_ENCODING = "UTF-8";
  private static final KryoPool KRYO_POOL        = new KryoPool.Builder(() -> {
    final Kryo kryo = new Kryo();
    //支持对象循环引用（否则会栈溢出）
    kryo.setReferences(true); //默认值就是 true，添加此行的目的是为了提醒维护者，不要改变这个配置

    //不强制要求注册类（注册行为无法保证多个 JVM 内同一个类的注册编号相同；而且业务系统中大量的 Class 也难以一一注册）
    kryo.setRegistrationRequired(false); //默认值就是 false，添加此行的目的是为了提醒维护者，不要改变这个配置

    kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(
        new StdInstantiatorStrategy()));
    return kryo;
  }).softReferences().build();


  /**
   * 获得一个 Kryo 实例
   *
   * @return 当前线程的 Kryo 实例
   */
  public static Kryo getInstance() {
    return KRYO_POOL.borrow();
  }

  //-----------------------------------------------
  //          序列化/反序列化对象，及类型信息
  //          序列化的结果里，包含类型的信息
  //          反序列化时不再需要提供类型
  //-----------------------------------------------

  /**
   * 将对象【及类型】序列化为字节数组
   *
   * @param obj 任意对象
   * @param <T> 对象的类型
   * @return 序列化后的字节数组
   */
  public static <T> byte[] writeToByteArray(T obj) {
    if (obj == null) {
      return null;
    }
    Kryo kryo = getInstance();
    try (
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Output output = new Output(byteArrayOutputStream)
    ) {

      kryo.writeClassAndObject(output, obj);
      output.flush();
      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new KryoException(e.getMessage(), e.getCause());
    } finally {
      KRYO_POOL.release(kryo);
    }
  }

  /**
   * 将对象【及类型】序列化为 String 利用了 Base64 编码
   *
   * @param obj 任意对象
   * @param <T> 对象的类型
   * @return 序列化后的字符串
   */
  public static <T> String writeToString(T obj) {
    try {
      if (obj == null) {
        return null;
      }
      return new String(Base64.encodeBase64(writeToByteArray(obj)), DEFAULT_ENCODING);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * 将字节数组反序列化为原对象
   *
   * @param byteArray writeToByteArray 方法序列化后的字节数组
   * @param <T>       原对象的类型
   * @return 原对象
   */
  @SuppressWarnings("unchecked")
  public static <T> T readFromByteArray(byte[] byteArray) {
    if (byteArray == null) {
      return null;
    }
    Kryo kryo = getInstance();
    try (
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray);
        Input input = new Input(byteArrayInputStream);
    ) {
      return (T) kryo.readClassAndObject(input);
    } catch (IOException e) {
      throw new KryoException(e.getMessage(), e.getCause());
    } finally {
      KRYO_POOL.release(kryo);
    }
  }

  /**
   * 将 String 反序列化为原对象 利用了 Base64 编码
   *
   * @param str writeToString 方法序列化后的字符串
   * @param <T> 原对象的类型
   * @return 原对象
   */
  public static <T> T readFromString(String str) {
    try {
      if (str == null) {
        return null;
      }
      return readFromByteArray(Base64.decodeBase64(str.getBytes(DEFAULT_ENCODING)));
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  //-----------------------------------------------
  //          只序列化/反序列化对象
  //          序列化的结果里，不包含类型的信息
  //-----------------------------------------------

  /**
   * 将对象序列化为字节数组
   *
   * @param obj 任意对象
   * @param <T> 对象的类型
   * @return 序列化后的字节数组
   */
  public static <T> byte[] writeObjectToByteArray(T obj) {
    if (obj == null) {
      return null;
    }
    Kryo kryo = getInstance();
    try (
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Output output = new Output(byteArrayOutputStream);
    ) {

      kryo.writeObject(output, obj);
      output.flush();

      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new KryoException(e.getMessage(), e.getCause());
    } finally {
      KRYO_POOL.release(kryo);
    }
  }

  /**
   * 将对象序列化为 String 利用了 Base64 编码
   *
   * @param obj 任意对象
   * @param <T> 对象的类型
   * @return 序列化后的字符串
   */
  public static <T> String writeObjectToString(T obj) {
    try {
      if (obj == null) {
        return null;
      }
      return new String(Base64.encodeBase64(writeObjectToByteArray(obj)), DEFAULT_ENCODING);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * 将字节数组反序列化为原对象
   *
   * @param byteArray writeToByteArray 方法序列化后的字节数组
   * @param clazz     原对象的 Class
   * @param <T>       原对象的类型
   * @return 原对象
   */
  public static <T> T readObjectFromByteArray(byte[] byteArray, Class<T> clazz) {
    if (byteArray == null) {
      return null;
    }
    Kryo kryo = getInstance();
    try (
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray);
        Input input = new Input(byteArrayInputStream);
    ) {
      return kryo.readObject(input, clazz);
    } catch (IOException e) {
      throw new KryoException(e.getMessage(), e.getCause());
    } finally {
      KRYO_POOL.release(kryo);
    }

  }

  /**
   * 将 String 反序列化为原对象 利用了 Base64 编码
   *
   * @param str   writeToString 方法序列化后的字符串
   * @param clazz 原对象的 Class
   * @param <T>   原对象的类型
   * @return 原对象
   */
  public static <T> T readObjectFromString(String str, Class<T> clazz) {
    try {
      if (str == null) {
        return null;
      }
      return readObjectFromByteArray(Base64.decodeBase64(str.getBytes(DEFAULT_ENCODING)), clazz);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }
}