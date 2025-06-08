package com.longhike.distributed_systems.map_reduce;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.longhike.common.Pair;

public class MapReduce {
  private final int numReducers;
  private final Pair<String, Integer> circuitBreaker;
  private final Map<String, Integer> outputMap;
  private final File[] textFiles;

  public MapReduce(int numReducers, File[] textFiles) {
    this.numReducers = numReducers;
    this.circuitBreaker = new Pair<>(null, -1);
    this.outputMap = new HashMap<>();
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
     * span mapper threads, then block
     * the main thread execution until they complete
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

  private Mapper[] getMappers(Reducer[] reducers, File[] files) {
    Mapper[] mappers = new Mapper[files.length];
    for (int i = 0; i < files.length; i++) {
      mappers[i] = new Mapper(files[i], reducers);
    }
    return mappers;
  }
}
