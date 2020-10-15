package io.github.natsusai.utils.i18n;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Locale.LanguageRange;
import java.util.Optional;

/**
 * i18n工具类
 *
 * @author Kurenai
 * @since 2020-08-25 13:17
 */

public class I18nUtils {

  /**
   * 获取Locale对象，不在范围内或是出错则返回null
   *
   * @param acceptLanguage Accept-Language
   * @return Locale对象
   */
  public static Locale getLocale(String acceptLanguage) {
    List<LanguageRange> languageRangeList;
    try {
      acceptLanguage    = Optional.ofNullable(acceptLanguage)
          .map(s -> s.replace("_", "-")).orElse("");
      languageRangeList = LanguageRange.parse(acceptLanguage);
    } catch (Exception e) {
      languageRangeList = new ArrayList<>();
    }
    return Optional.of(languageRangeList)
        .filter(list -> !list.isEmpty())
        .map(list -> list.get(0).getRange())
        .map(Locale::forLanguageTag).orElse(null);
  }

}
