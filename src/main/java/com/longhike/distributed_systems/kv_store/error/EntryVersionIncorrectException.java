package com.longhike.distributed_systems.kv_store.error;

public class EntryVersionIncorrectException extends Exception {
  public EntryVersionIncorrectException(String key, int version) {
    super("Could not update entry with key " + key + " because version " + version + " does not match the store.");
  }
}
