package io.github.natsusai.utils.io

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy
import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.pool.KryoPool
import org.apache.commons.codec.binary.Base64
import org.objenesis.strategy.StdInstantiatorStrategy
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.UnsupportedEncodingException
import java.nio.charset.StandardCharsets

/**
 * Kryo Utils
 * @author Kurenai
 * @since 2020-10-14
 */
class KryoUtils {
    companion object {
        private val DEFAULT_ENCODING = StandardCharsets.UTF_8
        private val KRYO_POOL: KryoPool = KryoPool.Builder {
            val kryo = Kryo()
            //支持对象循环引用（否则会栈溢出）
            kryo.references = true //默认值就是 true，添加此行的目的是为了提醒维护者，不要改变这个配置

            //不强制要求注册类（注册行为无法保证多个 JVM 内同一个类的注册编号相同；而且业务系统中大量的 Class 也难以一一注册）
            kryo.isRegistrationRequired = false //默认值就是 false，添加此行的目的是为了提醒维护者，不要改变这个配置
            kryo.instantiatorStrategy = DefaultInstantiatorStrategy(
                StdInstantiatorStrategy()
            )
            kryo
        }.softReferences().build()
    }

    /**
     * 获得一个 Kryo 实例
     *
     * @return 当前线程的 Kryo 实例
     */
    val instance: Kryo
        get() = KRYO_POOL.borrow()
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
    </T> */
    fun <T> writeToByteArray(obj: T): ByteArray {
        val kryo = instance
        try {
            ByteArrayOutputStream().use { byteArrayOutputStream ->
                Output(byteArrayOutputStream).use { output ->
                    kryo.writeClassAndObject(output, obj)
                    output.flush()
                    return byteArrayOutputStream.toByteArray()
                }
            }
        } catch (e: IOException) {
            throw KryoException(e.message, e.cause)
        } finally {
            KRYO_POOL.release(kryo)
        }
    }

    /**
     * 将对象【及类型】序列化为 String 利用了 Base64 编码
     *
     * @param obj 任意对象
     * @param <T> 对象的类型
     * @return 序列化后的字符串
    </T> */
    fun <T> writeToString(obj: T): String {
        return try {
            String(Base64.encodeBase64(writeToByteArray(obj)), DEFAULT_ENCODING)
        } catch (e: UnsupportedEncodingException) {
            throw IllegalStateException(e)
        }
    }

    /**
     * 将字节数组反序列化为原对象
     *
     * @param byteArray writeToByteArray 方法序列化后的字节数组
     * @param <T>       原对象的类型
     * @return 原对象
    </T> */
    fun <T> readFromByteArray(byteArray: ByteArray): T {
        val kryo = instance
        try {
            ByteArrayInputStream(byteArray).use { byteArrayInputStream ->
                Input(byteArrayInputStream).use { input ->
                    return kryo.readClassAndObject(
                        input
                    ) as T
                }
            }
        } catch (e: IOException) {
            throw KryoException(e.message, e.cause)
        } finally {
            KRYO_POOL.release(kryo)
        }
    }

    /**
     * 将 String 反序列化为原对象 利用了 Base64 编码
     *
     * @param str writeToString 方法序列化后的字符串
     * @param <T> 原对象的类型
     * @return 原对象
    </T> */
    fun <T> readFromString(str: String): T {
        return try {
            readFromByteArray(Base64.decodeBase64(str.toByteArray(DEFAULT_ENCODING)))
        } catch (e: UnsupportedEncodingException) {
            throw IllegalStateException(e)
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
    </T> */
    fun <T> writeObjectToByteArray(obj: T): ByteArray {
        val kryo = instance
        try {
            ByteArrayOutputStream().use { byteArrayOutputStream ->
                Output(byteArrayOutputStream).use { output ->
                    kryo.writeObject(output, obj)
                    output.flush()
                    return byteArrayOutputStream.toByteArray()
                }
            }
        } catch (e: IOException) {
            throw KryoException(e.message, e.cause)
        } finally {
            KRYO_POOL.release(kryo)
        }
    }

    /**
     * 将对象序列化为 String 利用了 Base64 编码
     *
     * @param obj 任意对象
     * @param <T> 对象的类型
     * @return 序列化后的字符串
    </T> */
    fun <T> writeObjectToString(obj: T): String {
        return try {
            String(Base64.encodeBase64(writeObjectToByteArray(obj)), DEFAULT_ENCODING)
        } catch (e: UnsupportedEncodingException) {
            throw IllegalStateException(e)
        }
    }

    /**
     * 将字节数组反序列化为原对象
     *
     * @param byteArray writeToByteArray 方法序列化后的字节数组
     * @param clazz     原对象的 Class
     * @param <T>       原对象的类型
     * @return 原对象
    </T> */
    fun <T> readObjectFromByteArray(byteArray: ByteArray, clazz: Class<T>): T {
        val kryo = instance
        try {
            ByteArrayInputStream(byteArray).use { byteArrayInputStream ->
                Input(byteArrayInputStream).use { input ->
                    return kryo.readObject(
                        input,
                        clazz
                    )
                }
            }
        } catch (e: IOException) {
            throw KryoException(e.message, e.cause)
        } finally {
            KRYO_POOL.release(kryo)
        }
    }

    /**
     * 将 String 反序列化为原对象 利用了 Base64 编码
     *
     * @param str   writeToString 方法序列化后的字符串
     * @param clazz 原对象的 Class
     * @param <T>   原对象的类型
     * @return 原对象
    </T> */
    fun <T> readObjectFromString(str: String, clazz: Class<T>): T {
        return try {
            readObjectFromByteArray(Base64.decodeBase64(str.toByteArray(DEFAULT_ENCODING)), clazz)
        } catch (e: UnsupportedEncodingException) {
            throw IllegalStateException(e)
        }
    }
}