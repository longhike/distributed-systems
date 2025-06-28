package com.longhike.kvstore;

import com.longhike.kvstore.error.EntryVersionIncorrectException;
import com.longhike.kvstore.error.NoEntryForKeyException;

public class Client {
  public Entry get(String key) {
    try {
      return Server.get(key);
    } catch (NoEntryForKeyException noEntryException) {
      System.out.println(noEntryException.getMessage());
      return null;
    }
  }

  public void put(String key, String value, int version) {
    putWithRetry(key, value, version, 0);
  }

  private void putWithRetry(String key, String value, int version, int tryCount) {
    try {
      if (tryCount < 3) {
        Server.put(key, value, version);
      }
    } catch (NoEntryForKeyException noEntryException) {
      System.out.println(noEntryException.getMessage());
      putWithRetry(key, value, 0, tryCount);

    } catch (EntryVersionIncorrectException incorrectVersionException) {
      System.out.println(incorrectVersionException.getMessage());
      putWithRetry(key, value, ++version, ++tryCount);
    }
  }

}
