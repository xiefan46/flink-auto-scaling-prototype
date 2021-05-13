package org.apache.flink.asc.store;

import com.linkedin.asc.store.Entry;
import com.linkedin.asc.store.KeyValueIterator;
import com.linkedin.asc.store.KeyValueStore;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.asc.exception.FlinkASCException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A key-value store that supports put, get, delete, backed by Flink {@link org.apache.flink.api.common.state.MapState}
 */
public class FlinkKeyValueStore<K, V> implements KeyValueStore<K, V> {

  private final MapState<K, V> flinkMapState;

  private static final Logger LOG = LoggerFactory.getLogger(FlinkKeyValueStore.class);

  public FlinkKeyValueStore(MapState<K, V> flinkMapState) {
    this.flinkMapState = flinkMapState;
  }

  @Override
  public V get(K key) {
    try{
      return flinkMapState.get(key);
    } catch (Exception e){
      throw new FlinkASCException(e.getCause());
    }
  }

  @Override
  public void put(K key, V value) {
    try {
      flinkMapState.put(key, value);
    } catch (Exception e) {
      throw new FlinkASCException(e.getCause());
    }
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    for(Entry<K, V> entry : entries){
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void delete(K key) {
    try {
      flinkMapState.remove(key);
    } catch (Exception e) {
      throw new FlinkASCException(e.getCause());
    }
  }

  @Override
  public KeyValueIterator<K, V> all() {
    try {
      Iterator<Map.Entry<K, V>> iterator = flinkMapState.iterator();
      return new KeyValueIterator<K, V>() {
        @Override
        public void close() {
          //not applicable
        }

        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public Entry<K, V> next() {
          Map.Entry<K, V> kv = iterator.next();
          return new Entry<K, V>(kv.getKey(), kv.getValue());
        }

        @Override
        public void remove() {
          iterator.remove();
        }

      };
    } catch (Exception e) {
      System.out.println(e.getStackTrace());
      throw new FlinkASCException(e.getCause());
    }
  }

  @Override
  public void close() {
    flinkMapState.clear();
  }
}
