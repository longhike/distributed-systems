package com.longhike.distributed_systems.map_reduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.AbstractMap.SimpleEntry;

public class Mapper implements Runnable {

  private final File file;
  private final Reducer[] reducers;

  public Mapper(File file, Reducer[] reducers) {
    this.file = file;
    this.reducers = reducers;
  }

  @Override
  public void run() {
    try (BufferedReader reader = new BufferedReader(new FileReader(this.file))) {
      String line;
      if ((line = reader.readLine()) != null) {
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
          // for the purposes of this example, no need to further process strings
          String word = tokenizer.nextToken().toLowerCase();
          SimpleEntry<String,Integer> entry = new SimpleEntry<>(word, 1);
          reducers[Math.abs(word.hashCode()) % this.reducers.length].put(entry);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
  }

}
