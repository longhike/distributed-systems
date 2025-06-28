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
        if (url.startsWith(startWith) && !url.contains("#")) {
          set.add(url);
        }
      }

      return new ArrayList<>(set);

    } catch (IOException e) {
      return new ArrayList<>();
    }
  }
}
