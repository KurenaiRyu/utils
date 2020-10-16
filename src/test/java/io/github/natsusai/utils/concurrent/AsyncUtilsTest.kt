package io.github.natsusai.utils.concurrent

import org.junit.Assert.*
import org.junit.Test
import java.util.concurrent.TimeUnit

class AsyncUtilsTest {
    @Test
    fun test() {
        val result = AsyncUtils.submit(1, TimeUnit.SECONDS) {
            Thread.sleep(2000)
            return@submit "test"
        }
        assertEquals(null, result)
    }
}