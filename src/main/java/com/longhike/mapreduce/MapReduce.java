package com.longhike.mapreduce;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.AbstractMap.SimpleEntry;

public class MapReduce {
  private final int numReducers;
  private final SimpleEntry<String, Integer> circuitBreaker;
  private final Map<String, Integer> outputMap;
  private final String[] texts;

  public static void main(String[] args) {
    try {
      new MapReduce(8).execute();
    } catch (InterruptedException exception) {
      System.out.println(exception.getMessage());
    }

  }

  public MapReduce(int numReducers) {
    this.numReducers = numReducers;
    this.circuitBreaker = new SimpleEntry<>(null, -1);
    this.outputMap = new HashMap<>();
    this.texts = Sources.texts;
  }

  public void execute() throws InterruptedException {
    Reducer[] reducers = getReducers();
    Mapper[] mappers = getMappers(reducers, texts);

    // Executor services for mapper and reducer tasks
    ExecutorService mapperExecutor = Executors.newFixedThreadPool(texts.length);
    ExecutorService reducerExecutor = Executors.newFixedThreadPool(numReducers);

    // submit the mapper and reducer tasks to their respective executors
    List<Future<?>> reducerFutures = submitTasks(reducerExecutor, reducers);
    List<Future<?>> mapperFutures = submitTasks(mapperExecutor, mappers);
    
    // cleanup
    mapperExecutor.shutdown();
    reducerExecutor.shutdown();

    // block main thread until all mappers have completed
    waitForAll(mapperFutures);
    // enqueue circuit breaker on reducers
    for (Reducer reducer : reducers) {
      reducer.put(circuitBreaker);
    }
    // block main thread until all reducers have completed
    waitForAll(reducerFutures);

    // print result
    System.out.println("------------- RESULTS!! -----------------");
    outputMap.forEach((k, v) -> System.out.println(k + ": " + v));
  }

  private Reducer[] getReducers() {
    Reducer[] reducers = new Reducer[numReducers];
    for (int i = 0; i < numReducers; i++) {
      reducers[i] = new Reducer(outputMap, circuitBreaker);
    }
    return reducers;
  }

  private Mapper[] getMappers(Reducer[] reducers, String[] texts) {
    Mapper[] mappers = new Mapper[texts.length];
    for (int i = 0; i < texts.length; i++) {
      mappers[i] = new Mapper(texts[i], reducers);
    }
    return mappers;
  }

  private List<Future<?>> submitTasks(ExecutorService executor, Runnable[] tasks) {
    List<Future<?>> futures = new ArrayList<>();
    for (Runnable task : tasks) {
      futures.add(executor.submit(task));
    }
    return futures;
  }

  private void waitForAll(List<Future<?>> futures) throws InterruptedException {
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        Thread.currentThread().interrupt();
        throw new InterruptedException("Task execution failed: " + e.getMessage());
      }
    }
  }
}
