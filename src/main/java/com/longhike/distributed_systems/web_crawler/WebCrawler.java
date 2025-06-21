package com.longhike.distributed_systems.web_crawler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class WebCrawler {
  private final String BASE_URL = "https://en.wikipedia.org/wiki";
  private final int THREAD_COUNT = 50;

  public WebCrawler() {
  }

  public void crawl(String startingPoint, String target) {
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
    ConcurrentHashMap<String, String> pathMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Boolean> seen = new ConcurrentHashMap<>();
    AtomicBoolean found = new AtomicBoolean(false);

    queue.add(startingPoint);
    pathMap.put(startingPoint, startingPoint);
    seen.put(startingPoint, true);

    while (!queue.isEmpty() && !found.get()) {
      List<CompletableFuture<Void>> tasks = new ArrayList<>();
      int batch = Math.min(THREAD_COUNT, queue.size());
      for (int i = 0; i < batch; i++) {
        String current = queue.poll();
        if (current != null) {
          CompletableFuture<Void> task = CompletableFuture.runAsync(
              () -> processUrl(current, target, queue, seen, pathMap, found),
              executor).orTimeout(10, TimeUnit.SECONDS)
              .exceptionally(ex -> {
                System.err.println("Error processing URL: " + ex.getMessage());
                return null;
              });
          tasks.add(task);
        }
      }
      try {
        CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0]))
            .get(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        System.err.println("Batch processing timed out: " + e.getMessage());
      }
    }

    executor.shutdown();

    if (found.get()) {
      System.out.println("---------- RESULT -----------");
      System.out.println(reconstructPath(pathMap, startingPoint, target));
    } else {
      System.out.println("-------- No path found --------");
    }

  }

  private void processUrl(
      String current,
      String target,
      ConcurrentLinkedQueue<String> queue,
      ConcurrentHashMap<String, Boolean> seen,
      ConcurrentHashMap<String, String> pathMap,
      AtomicBoolean found) {
    System.out.println("Processing: " + current);
    if (found.get()) {
      return;
    }
    if (current.equals(target)) {
      found.set(true);
      return;
    }

    List<String> urls = UrlExtractor.getChildUrls(current, BASE_URL);

    for (String url : urls) {
      if (seen.putIfAbsent(url, true) == null) {
        queue.add(url);
        pathMap.put(url, current);
      }
    }

  }

  private String reconstructPath(Map<String, String> pathMap, String start, String end) {
    List<String> path = new ArrayList<>();
    String current = end;

    while (!current.equals(start)) {
      path.add(current);
      current = pathMap.get(current);
    }

    path.add(start);
    Collections.reverse(path);

    return String.join(" -> ", path);
  }
}
