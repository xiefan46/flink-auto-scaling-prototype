package com.linkedin.asc.store;

import java.util.Iterator;
import java.util.Map;


public interface KeyValueIterator<K, V> extends Iterator<Map.Entry<K, V>> {
  public void close();
}
