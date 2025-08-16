package com.longhike.webcrawler;

import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class WebCrawler {
  private final String BASE_URL = "https://en.wikipedia.org/wiki";
  private final int THREAD_COUNT = 50;
  RateLimiter rateLimiter = RateLimiter.create(2);

  public static void main(String[] args) {
    new WebCrawler()
        .crawl("https://en.wikipedia.org/wiki/England", "https://en.wikipedia.org/wiki/Jesus");
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

    // race condition using queue as condition - use semaphore
    while (!queue.isEmpty() && !found.get()) {
      List<CompletableFuture<Void>> tasks = new ArrayList<>();
      int batch = Math.min(THREAD_COUNT, queue.size());
      for (int i = 0; i < batch; i++) {
        String current = queue.poll();
        if (current != null) {
          CompletableFuture<Void> task =
              this.runCancellable(
                      executor, () -> processUrl(current, target, queue, seen, pathMap, found), 10)
                  .exceptionally(
                      ex -> {
                        System.err.println("Error processing URL: " + ex.getMessage());
                        return null;
                      });
          tasks.add(task);
        }
      }
      try {
        CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
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

    if (Thread.currentThread().isInterrupted()) {
      return;
    }

    rateLimiter.acquire();

    List<String> urls = UrlExtractor.getChildUrls(current, BASE_URL);

    if (Thread.currentThread().isInterrupted()) {
      return;
    }

    for (String url : urls) {
      if (seen.putIfAbsent(url, true) == null) {
        queue.add(url);
        pathMap.put(url, current);
      }
    }
  }

  private CompletableFuture<Void> runCancellable(
      ExecutorService executor, Runnable task, int timeoutSeconds) {
    Future<?> f = executor.submit(task);

    CompletableFuture<Void> cf = new CompletableFuture<>();

    try {
      f.get(timeoutSeconds, TimeUnit.SECONDS);
      cf.complete(null);
    } catch (TimeoutException e) {
      f.cancel(true);
      cf.completeExceptionally(e.getCause());
    } catch (ExecutionException e) {
      // no need to cancel the future; its error-caused cancellation is what caused the exception
      cf.completeExceptionally(e.getCause());
    } catch (InterruptedException e) {
      f.cancel(true);
      Thread.currentThread().interrupt();
      cf.completeExceptionally(e.getCause());
    }

    return cf;
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
