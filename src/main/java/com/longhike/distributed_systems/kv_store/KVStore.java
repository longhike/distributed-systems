package com.longhike.distributed_systems.kv_store;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KVStore {
    // Common pool of 1000 keys
    private static final String[] KEYS = new String[1000];
    
    // Initialize keys
    static {
        for (int i = 0; i < 1000; i++) {
            KEYS[i] = "key-" + i;
        }
    }
    
    public static void doKVStore() {
        System.out.println("Starting KV Store stress test with 100 clients...");
        
        // Create and start 100 client threads
        ExecutorService executor = Executors.newFixedThreadPool(100);
        
        for (int i = 0; i < 100; i++) {
            executor.submit(new ClientTask(i));
        }
        
        // Run for a specified duration
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
        
        System.out.println("KV Store stress test completed");
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
                
                // Run operations for a specified number of iterations
                for (int i = 0; i < 1000; i++) {
                    // Randomly choose between get and put operations
                    if (random.nextBoolean()) {
                        performGet();
                    } else {
                        performPut();
                    }
                    
                    // Small delay between operations to prevent CPU saturation
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
            // Simple logging for demonstration
            if (entry != null) {
                System.out.println("Client " + clientId + " GET: " + key + " = " + entry.getValue() + " (v" + entry.getVersion() + ")");
            } else {
                System.out.println("Client " + clientId + " GET: " + key + " = null");
            }
        }
        
        private void performPut() {
            String key = KEYS[random.nextInt(KEYS.length)];
            String value = generateValue();
            
            // First try to get the current version
            Entry currentEntry = client.get(key);
            int version = (currentEntry != null) ? currentEntry.getVersion() : 0;
            
            client.put(key, value, version);
            System.out.println("Client " + clientId + " PUT: " + key + " = " + value + " (v" + version + ")");
        }
        
        private String generateValue() {
            // Generate a dynamic value that includes client ID, timestamp, etc.
            return "client-" + clientId + "-time-" + System.currentTimeMillis() + 
                   "-random-" + random.nextInt(1000);
        }
    }
}
