package com.longhike.distributed_systems.kv_store.error;

public class NoEntryForKeyException extends Exception {
  public NoEntryForKeyException(String key) {
    super("No entry found for key: " + key + ".");
  }
}