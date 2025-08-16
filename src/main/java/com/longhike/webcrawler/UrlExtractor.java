package com.longhike.webcrawler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class UrlExtractor {

  public static List<String> getChildUrls(String base, String startWith) {
    try {
      Document page = Jsoup.connect(base).get();
      Elements elements = page.select("a[href]");

      Set<String> set = new HashSet<>();
      for (Element element : elements) {
        String url = element.attr("abs:href");
        if (url.startsWith(startWith)
            && !url.contains("#")
            && !url.contains("_(identifier)")
            && !url.contains("Wikipedia:Link_rot")
            && !url.contains("Special:")
            && !url.contains("Help:")
            && !url.contains("Category:")
            && !url.contains("Wikipedia:")
            && !url.contains("File:")
            && !url.contains("Portal:")
            && !url.contains("Main_Page")
            && !url.contains("Template:")
            && !url.contains("Template_talk:")) {
          set.add(url);
        }
      }

      return new ArrayList<>(set);

    } catch (IOException e) {
      return new ArrayList<>();
    }
  }
}
