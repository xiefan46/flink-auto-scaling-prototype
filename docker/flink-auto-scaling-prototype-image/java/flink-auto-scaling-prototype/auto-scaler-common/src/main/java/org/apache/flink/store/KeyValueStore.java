package org.apache.flink.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A key-value store interface that supports put, get, delete
 *
 * @param <K> the type of keys maintained by this key-value store.
 * @param <V> the type of values maintained by this key-value store.
 */
public interface KeyValueStore<K, V> {
  /**
   * Gets the value associated with the specified {@code key}.
   *
   * @param key the key with which the associated value is to be fetched.
   * @return if found, the value associated with the specified {@code key}; otherwise, {@code null}.
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  V get(K key);

  /**
   * Gets the values with which the specified {@code keys} are associated.
   *
   * @param keys the keys with which the associated values are to be fetched.
   * @return a map of the keys that were found and their respective values.
   * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
   */
  default Map<K, V> getAll(List<K> keys) {
    Map<K, V> map = new HashMap<>(keys.size());

    for (K key : keys) {
      V value = get(key);

      if (value != null) {
        map.put(key, value);
      }
    }

    return map;
  }

  /**
   * Updates the mapping of the specified key-value pair; Associates the specified {@code key} with the specified {@code value}.
   *
   * @param key the key with which the specified {@code value} is to be associated.
   * @param value the value with which the specified {@code key} is to be associated.
   * @throws NullPointerException if the specified {@code key} or {@code value} is {@code null}.
   */
  void put(K key, V value);

  /**
   * Updates the mappings of the specified key-value {@code entries}.
   *
   * @param entries the updated mappings to put into this key-value store.
   * @throws NullPointerException if any of the specified {@code entries} has {@code null} as key or value.
   */
  void putAll(List<Map.Entry<K, V>> entries);

  /**
   * Deletes the mapping for the specified {@code key} from this key-value store (if such mapping exists).
   *
   * @param key the key for which the mapping is to be deleted.
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  void delete(K key);

  /**
   * Deletes the mappings for the specified {@code keys} from this key-value store (if such mappings exist).
   *
   * @param keys the keys for which the mappings are to be deleted.
   * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
   */
  default void deleteAll(List<K> keys) {
    for (K key : keys) {
      delete(key);
    }
  }


  /**
   * Returns an iterator for all entries in this key-value store.
   *
   * <p><b>API Note:</b> The returned iterator MUST be closed after use.</p>
   * @return an iterator for all entries in this key-value store.
   */
  KeyValueIterator<K, V> all();

  /**
   * Closes this key-value store, if applicable, relinquishing any underlying resources.
   */
  void close();



}

