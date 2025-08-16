package com.longhike.kvstore.error;

public class NoEntryForKeyException extends Exception {
  public NoEntryForKeyException(String key) {
    super("No entry found for key: " + key + ".");
  }
}
