package com.longhike.common;

public class Pair<K, V> {
  private final K key;
  private final V value;

  public Pair(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K key() {
    return key;
  }

  public V value() {
    return value;
  }

  public boolean equals(Pair<K, V> pair) {
    return this.value == pair.value() && this.key == pair.key();
  }
}
