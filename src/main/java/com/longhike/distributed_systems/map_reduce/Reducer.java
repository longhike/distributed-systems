package com.longhike.distributed_systems.map_reduce;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.longhike.common.Pair;

public class Reducer implements Runnable {
  private final BlockingQueue<Pair<String, Integer>> queue;
  private final Map<String, Integer> map;
  private final Pair<String, Integer> circuitBreaker;
  private final Map<String, Integer> output;

  public Reducer(Map<String, Integer> output, Pair<String, Integer> circuitBreaker) {
    this.queue = new LinkedBlockingQueue<>();
    this.map = new HashMap<>();
    this.circuitBreaker = circuitBreaker;
    this.output = output;
  }

  public boolean put(Pair<String, Integer> entry) {
    return this.queue.add(entry);
  }

  @Override
  public void run() {
    try {
      while (true) {
        Pair<String, Integer> entry = queue.take();
        if (entry.equals(circuitBreaker)) {
          break;
        }
        map.merge(entry.key(), entry.value(), Integer::sum);
      }
      this.writeOutput();
    } catch (Exception e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
  }

  private void writeOutput() {
    synchronized (Reducer.class) {
      output.putAll(map);
    }
  }
}
