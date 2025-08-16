package com.longhike.kvstore;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KVStore {
  private static final int NUM_KEYS = 1000;
  private static final int NUM_THREADS = 100;
  private static final int NUM_ITERATIONS_PER_CLIENT = 1000;
  private static final String[] KEYS = new String[NUM_KEYS];

  static {
    for (int i = 0; i < NUM_KEYS; i++) {
      KEYS[i] = "key-" + i;
    }
  }

  public static void main(String[] args) {
    System.out.println("Starting KV Store stress test with 100 clients...");

    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

    for (int i = 0; i < 100; i++) {
      executor.submit(new ClientTask(i));
    }

    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
        System.out.println("Test timeout reached. Shutting down...");
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      System.out.println("Test interrupted");
      executor.shutdownNow();
    }

    System.out.println("Stress test completed");
  }

  static class ClientTask implements Runnable {
    private final int clientId;
    private final Client client;
    private final Random random = new Random();

    public ClientTask(int clientId) {
      this.clientId = clientId;
      this.client = new Client();
    }

    @Override
    public void run() {
      try {
        System.out.println("Client " + clientId + " started");

        for (int i = 0; i < NUM_ITERATIONS_PER_CLIENT; i++) {
          if (random.nextBoolean()) {
            performGet();
          } else {
            performPut();
          }

          Thread.sleep(random.nextInt(10));
        }

        System.out.println("Client " + clientId + " finished");
      } catch (InterruptedException e) {
        System.out.println("Client " + clientId + " interrupted");
        Thread.currentThread().interrupt();
      }
    }

    private void performGet() {
      String key = KEYS[random.nextInt(KEYS.length)];
      Entry entry = client.get(key);
      if (entry != null) {
        System.out.println(
            "Client "
                + clientId
                + " GET: "
                + key
                + " = "
                + entry.getValue()
                + " (v"
                + entry.getVersion()
                + ")");
      } else {
        System.out.println("Client " + clientId + " GET: " + key + " = null");
      }
    }

    private void performPut() {
      String key = KEYS[random.nextInt(KEYS.length)];
      String value = generateValue();

      Entry currentEntry = client.get(key);
      int version = (currentEntry != null) ? currentEntry.getVersion() : 0;

      client.put(key, value, version);
      System.out.println(
          "Client " + clientId + " PUT: " + key + " = " + value + " (v" + version + ")");
    }

    private String generateValue() {
      return "client-"
          + clientId
          + "-time-"
          + System.currentTimeMillis()
          + "-random-"
          + random.nextInt(NUM_KEYS);
    }
  }
}
