package io.github.natsusai.utils.i18n

import org.junit.Test

import org.junit.Assert.*
import java.util.*

class I18nUtilsTest {

    @Test
    fun getLocale() {
        assertEquals(null, I18nUtils.getLocale(""));
        assertEquals(Locale.SIMPLIFIED_CHINESE, I18nUtils.getLocale("zh-cn"))
    }
}