package com.longhike.kvstore;

import com.longhike.kvstore.error.EntryVersionIncorrectException;
import com.longhike.kvstore.error.NoEntryForKeyException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class Server {
  private static final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, ReentrantLock> lockStore =
      new ConcurrentHashMap<>();

  public static Entry get(String key) throws NoEntryForKeyException {
    // no lock - return whatever is currently available, even if outdated.
    Entry entry = store.get(key);
    if (entry == null) {
      throw new NoEntryForKeyException(key);
    }
    return entry;
  }

  public static void put(String key, String value, int version)
      throws NoEntryForKeyException, EntryVersionIncorrectException {
    /**
     * theoratically, this is unnecessary. we could use store.compute to make this atomic. but to
     * mimic a distributed system, where we'd need to acquire locks manually, we use a manual
     * locking scheme here.
     */
    ReentrantLock lock = lockStore.computeIfAbsent(key, (k) -> new ReentrantLock());
    lock.lock();
    try {
      Entry entry = store.get(key);
      if (entry == null) {
        if (version > 0) {
          throw new NoEntryForKeyException(key);
        }
        store.put(key, new Entry(value, 1));
        return;
      }
      if (version != entry.getVersion()) {
        throw new EntryVersionIncorrectException(key, version);
      }
      store.put(key, new Entry(value, version + 1));
    } finally {
      lock.unlock();
    }
  }
}
