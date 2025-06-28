package com.longhike.kvstore;

public final class Entry {
  private String value;
  private int version;

  public Entry(String value, int version) {
    this.value = value;
    this.version = version;
  }

  public String getValue() {
    return value;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    result = prime * result + version;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Entry other = (Entry) obj;
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equals(other.value))
      return false;
    if (version != other.version)
      return false;
    return true;
  }
}
