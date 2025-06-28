package com.longhike.mapreduce;

import java.util.StringTokenizer;
import java.util.AbstractMap.SimpleEntry;

public class Mapper implements Runnable {

  private final String text;
  private final Reducer[] reducers;

  public Mapper(String text, Reducer[] reducers) {
    this.text = text;
    this.reducers = reducers;
  }

  @Override
  public void run() {
    StringTokenizer tokenizer = new StringTokenizer(text);

    while (tokenizer.hasMoreTokens()) {
      // for the purposes of this example, no need to further process strings
      String word = tokenizer.nextToken().toLowerCase();
      SimpleEntry<String, Integer> entry = new SimpleEntry<>(word, 1);
      reducers[Math.abs(word.hashCode()) % this.reducers.length].put(entry);
    }

  }

}
