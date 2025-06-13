package com.longhike.distributed_systems.map_reduce;

import java.util.HashMap;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Reducer implements Runnable {
  private final BlockingQueue<SimpleEntry<String, Integer>> queue;
  private final Map<String, Integer> map;
  private final SimpleEntry<String, Integer> circuitBreaker;
  private final ConcurrentHashMap<String, Integer> output;

  public Reducer(ConcurrentHashMap<String, Integer> output, SimpleEntry<String, Integer> circuitBreaker) {
    this.queue = new LinkedBlockingQueue<>();
    this.map = new HashMap<>();
    this.circuitBreaker = circuitBreaker;
    this.output = output;
  }

  public boolean put(SimpleEntry<String, Integer> entry) {
    return this.queue.add(entry);
  }

  @Override
  public void run() {
    try {
      while (true) {
        SimpleEntry<String, Integer> entry = queue.take();
        if (entry.equals(circuitBreaker)) {
          break;
        }
        map.merge(entry.getKey(), entry.getValue(), Integer::sum);
      }
      this.writeOutput();
    } catch (Exception e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
  }

  private void writeOutput() {
    for (Entry<String, Integer> entry : map.entrySet()) {
      /**
       * we can use put here, because we can guarantee,
       * due to the way we assign keys to each reducer thread,
       * that only this Reducer will have these keys.
       */
      output.put(entry.getKey(), entry.getValue());
    }
  }
}
