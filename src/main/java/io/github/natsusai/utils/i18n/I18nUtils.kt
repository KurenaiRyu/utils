package io.github.natsusai.utils.i18n

import java.util.*
import java.util.Locale.LanguageRange

/**
 * i18n工具类
 *
 * @author Kurenai
 * @since 2020-08-25 13:17
 */
class I18nUtils {
    /**
     * 获取Locale对象，不在范围内或是出错则返回null
     *
     * @param acceptLanguage Accept-Language
     * @return Locale对象
     */
    fun getLocale(acceptLanguage: String?): Locale? {
        var acceptLanguage = acceptLanguage
        var languageRangeList: List<LanguageRange>
        try {
            acceptLanguage = Optional.ofNullable(acceptLanguage)
                .map { s: String -> s.replace("_", "-") }.orElse("")
            languageRangeList = LanguageRange.parse(acceptLanguage)
        } catch (e: Exception) {
            languageRangeList = ArrayList()
        }
        return Optional.of(languageRangeList)
            .filter { list: List<LanguageRange> -> !list.isEmpty() }
            .map { list: List<LanguageRange> -> list[0].range }
            .map { languageTag: String? -> Locale.forLanguageTag(languageTag) }.orElse(null)
    }
}