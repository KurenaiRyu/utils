package io.github.natsusai.utils.concurrent

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import java.util.concurrent.TimeUnit

class AsyncUtilsTest {
    @Test
    fun testTimeOut() {
        val result = AsyncUtils.submit(1, TimeUnit.SECONDS) {
            Thread.sleep(2000)
            return@submit "test"
        }
        assertNull(result)
    }

    @Test
    fun testNormal() {
        val result = AsyncUtils.submit(2, TimeUnit.SECONDS) {
            Thread.sleep(1000)
            return@submit "test"
        }
        assertEquals("test", result)
    }
}