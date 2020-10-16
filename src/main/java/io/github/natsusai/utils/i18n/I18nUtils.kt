package io.github.natsusai.utils.i18n

import java.util.*
import java.util.Locale.LanguageRange
import java.util.Locale.getDefault

/**
 * i18n工具类
 *
 * @author Kurenai
 * @since 2020-08-25 13:17
 */
class I18nUtils {

    companion object {
        /**
         * 获取Locale对象，不在范围内或是出错则返回null
         *
         * @param acceptLanguage Accept-Language
         * @return Locale对象
         */
        fun getLocale(acceptLanguage: String): Locale? {
            val languageRangeList: List<LanguageRange> = try {
                LanguageRange.parse(acceptLanguage.replace("_", "-"))
            } catch (e: Exception) {
                ArrayList()
            }

            return languageRangeList.firstOrNull()?.range?.let { Locale.forLanguageTag(it) }
        }
    }
}