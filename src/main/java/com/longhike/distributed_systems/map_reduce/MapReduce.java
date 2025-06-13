package com.longhike.distributed_systems.map_reduce;

import java.io.File;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.ConcurrentHashMap;

public class MapReduce {
  private final int numReducers;
  private final SimpleEntry<String, Integer> circuitBreaker;
  private final ConcurrentHashMap<String, Integer> outputMap;
  private final File[] textFiles;

  public MapReduce(int numReducers, File[] textFiles) {
    this.numReducers = numReducers;
    this.circuitBreaker = new SimpleEntry<>(null, -1);
    this.outputMap = new ConcurrentHashMap<>();
    this.textFiles = textFiles;
  }

  public void execute() throws InterruptedException {
    Reducer[] reducers = getReducers();
    Mapper[] mappers = getMappers(reducers, textFiles);

    Thread[] reducerThreads = new Thread[reducers.length];
    Thread[] mapperThreads = new Thread[mappers.length];

    // spawn reducer threads
    for (int i = 0; i < reducers.length; i++) {
      reducerThreads[i] = new Thread(reducers[i]);
      reducerThreads[i].start();
    }

    /**
     * spawn mapper threads, then block
     * the main thread execution until they complete
     */
    for (int i = 0; i < mappers.length; i++) {
      mapperThreads[i] = new Thread(mappers[i]);
      mapperThreads[i].start();
    }

    for (Thread mt : mapperThreads) {
      mt.join();
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

  private Mapper[] getMappers(Reducer[] reducers, File[] files) {
    Mapper[] mappers = new Mapper[files.length];
    for (int i = 0; i < files.length; i++) {
      mappers[i] = new Mapper(files[i], reducers);
    }
    return mappers;
  }
}
