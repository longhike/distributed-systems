package com.longhike.mapreduce;

import java.util.HashMap;
import java.util.Map;
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

    Thread[] reducerThreads = new Thread[reducers.length];
    Thread[] mapperThreads = new Thread[mappers.length];

    // spawn reducer threads
    for (int i = 0; i < reducers.length; i++) {
      reducerThreads[i] = new Thread(reducers[i]);
      reducerThreads[i].start();
    }

    /**
     * spawn mapper threads, then block
     * the main thread's execution until they complete
     */
    for (int i = 0; i < mappers.length; i++) {
      mapperThreads[i] = new Thread(mappers[i]);
      mapperThreads[i].start();
      mapperThreads[i].join();
    }

    /**
     * enque the circuitbreaker on the reducers once mappers complete
     */
    for (Reducer reducer : reducers) {
      reducer.put(circuitBreaker);
    }

    /**
     * block main thread execution until reducers complete
     */
    for (Thread reducerThread : reducerThreads) {
      reducerThread.join();
    }

    System.out.println("------------------------------");
    for (Map.Entry<String, Integer> entry : outputMap.entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
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
}
